################################################################################
# Copyright (c) 2020, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import argparse
import asyncio
import concurrent.futures
import contextlib
import logging
import os
import pathlib
import socket
import subprocess
import threading
import time
import urllib.parse

import pytest
import aiokatcp
import boto
import requests
import katsdptelstate

import katsdpmetawriter


class S3User:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key


pytestmark = [pytest.mark.asyncio]
CBID = '1122334455'
STREAM_NAME = 'sdp_l0'

ADMIN_USER = S3User('test-access-key', 'test-secret-key')
NOBODY_USER = S3User('nobody-access-key', 'nobody-secret-key')
READONLY_USER = S3User('readonly-access-key', 'readonly-secret-key')

# MinIO doesn't allow completely empty policies, so we give it an arbitrary
# action.
NOBODY_POLICY = '''
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:HeadBucket"],
            "Resource": ["arn:aws:s3:::*"]
        }
    ]
}
'''
# The minio default readonly policy doesn't allow get_canonical_user_id
# (which is implemented in terms of ListBuckets), so we need to augment it.
READONLY_POLICY = '''
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketLocation",
                "s3:GetObject",
                "s3:ListAllMyBuckets"
            ],
            "Resource": ["arn:aws:s3:::*"]
        }
    ]
}
'''


class S3Server:
    def __init__(self, path, user):
        self.host = '127.0.0.1'      # Unlike 'localhost', ensures IPv4
        self.path = path
        self.user = user
        self._process = None

        env = os.environ.copy()
        env['MINIO_BROWSER'] = 'off'
        env['MINIO_ACCESS_KEY'] = self.user.access_key
        env['MINIO_SECRET_KEY'] = self.user.secret_key
        with contextlib.ExitStack() as exit_stack:
            sock = exit_stack.enter_context(socket.socket())
            # Allows minio to bind to the same socket. Setting both
            # SO_REUSEPORT and SO_REUSEADDR might not be necessary, but
            # could make this more portable.
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
            self._process = subprocess.Popen(
                [
                    'minio', 'server', '--quiet',
                    '--address', f'{self.host}:{self.port}',
                    '-C', str(self.path / 'config'),
                    str(self.path / 'data'),
                ],
                stdout=subprocess.DEVNULL,
                env=env
            )

            self.url = f'http://{self.host}:{self.port}'
            self.auth_url = f'http://{user.access_key}:{user.secret_key}@{self.host}:{self.port}'
            health_url = urllib.parse.urljoin(self.url, '/minio/health/live')
            for i in range(100):
                try:
                    with requests.get(health_url) as resp:
                        if resp.ok:
                            break
                except requests.ConnectionError:
                    pass
                if self._process.poll() is not None:
                    raise RuntimeError('Minio died before it became healthy')
                time.sleep(0.1)
            else:
                raise RuntimeError('Timed out waiting for minio to be ready')

    def wipe(self):
        """Remove all buckets and objects"""
        self.mc('rb', '--force', '--dangerous', 'minio')

    def close(self):
        if self._process:
            self._process.terminate()
            self._process.wait()
            self._process = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()

    def mc(self, *args):
        """Run a (minio) mc admin subcommand against the running server.

        The running server has the alias ``minio``.
        """
        env = os.environ.copy()
        env['MC_HOST_minio'] = self.auth_url
        # --config-dir is set just to prevent any config set by the user
        # from interfering with the test.
        subprocess.run(
            [
                'mc', '--quiet', '--no-color', f'--config-dir={self.path}',
                *args
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=env,
            encoding='utf-8',
            errors='replace',
            check=True
        )


@pytest.fixture(scope='session')
def _s3_server(tmp_path_factory):
    path = tmp_path_factory.mktemp('minio')
    try:
        server = S3Server(path, ADMIN_USER)
    except OSError as exc:
        pytest.skip(f'Could not start minio: {exc}')

    policy_dir = tmp_path_factory.mktemp('policies')
    (policy_dir / "readonly.json").write_text(READONLY_POLICY)
    (policy_dir / "nobody.json").write_text(NOBODY_POLICY)
    with server:
        try:
            server.mc(
                'admin', 'user', 'add', 'minio', NOBODY_USER.access_key, NOBODY_USER.secret_key
            )
            server.mc(
                'admin', 'user', 'add', 'minio', READONLY_USER.access_key, READONLY_USER.secret_key
            )
            server.mc(
                'admin', 'policy', 'add', 'minio', 'nobody', str(policy_dir / "nobody.json")
            )
            server.mc(
                'admin', 'policy', 'add', 'minio', 'readonly2', str(policy_dir / "readonly.json")
            )
            server.mc(
                'admin', 'policy', 'set', 'minio', 'nobody', f'user={NOBODY_USER.access_key}'
            )
            server.mc(
                'admin', 'policy', 'set', 'minio', 'readonly2', f'user={READONLY_USER.access_key}'
            )
        except OSError as exc:
            pytest.skip(f'Could not run mc: {exc}')
        except subprocess.CalledProcessError as exc:
            pytest.fail(f'Failed to set up extra users: {exc.stderr}')

        yield server


@pytest.fixture
def s3_server(_s3_server):
    """Start a process running an S3 server.

    This relies on `minio` and `mc` being installed on the :envvar:`PATH`. If
    they are not found, the test will be skipped.

    The value of the fixture is an instance of :class:`S3Server`. It also has
    additional users, with credentials given by :data:`NOBODY_USER` and
    :data:`READONLY_USER`.

    .. warning::

       The server is only started once per session, and is wiped of data before
       each test. Any state not associated with buckets or objects will leak
       between tests.
    """
    try:
        _s3_server.wipe()
    except subprocess.CalledProcessError as exc:
        pytest.fail(f'Failed to wipe out objects: {exc.stderr}')
    return _s3_server


@pytest.fixture
def s3_args(s3_server):
    return argparse.Namespace(
        access_key=s3_server.user.access_key,
        secret_key=s3_server.user.secret_key,
        s3_host=s3_server.host,
        s3_port=s3_server.port
    )


@pytest.fixture
def telstate():
    """Telescope state with a smattering of keys for test purposes"""
    telstate = katsdptelstate.TelescopeState()
    telstate[f'{STREAM_NAME}_int_time'] = 7.5
    telstate[f'{STREAM_NAME}_stream_type'] = 'sdp.vis'
    telstate['sub_pool_resources'] = 'cbf_1,sdp_1,m000'
    telstate['m000_observer'] = 'm000, -30:42:39.8, 21:26:38.0, 1035.0, 13.5, -8.258 -207.289 1.2075 5874.184 5875.444, -0:00:39.7 0 -0:04:04.4 -0:04:53.0 0:00:57.8 -0:00:13.9 0:13:45.2 0:00:59.8, 1.14'     # noqa: E501
    telstate.add(f'{CBID}_obs_activity', 'slew', ts=1234567890.0)
    telstate[f'{CBID}_{STREAM_NAME}_first_timestamp'] = 1122334455.0
    telstate.add('s0000_activity', 'slew', ts=1234567890.0)
    telstate[f'{STREAM_NAME}_another_attrib'] = 'foo'
    telstate.add('another_sensor', 'value', ts=1234567890.0)
    telstate['sdp_archived_streams'] = ['sdp_l0']
    return telstate


def test_get_s3_connection(s3_args):
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    conn = katsdpmetawriter.get_s3_connection(boto_dict)
    assert conn is not None
    conn.close()


def test_get_s3_connection_bad_access_key(s3_args, caplog):
    s3_args.access_key = 'wrong'
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    with caplog.at_level(logging.ERROR):
        conn = katsdpmetawriter.get_s3_connection(boto_dict)
    assert conn is None
    assert 'not a valid S3 user' in caplog.text


def test_get_s3_connection_bad_secret_key(s3_args, caplog):
    s3_args.secret_key = 'wrong'
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    with caplog.at_level(logging.ERROR):
        conn = katsdpmetawriter.get_s3_connection(boto_dict)
    assert conn is None
    assert 'secret key is not valid' in caplog.text


def test_get_s3_connection_no_permissions(s3_args, caplog):
    s3_args.access_key = NOBODY_USER.access_key
    s3_args.secret_key = NOBODY_USER.secret_key
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    with caplog.at_level(logging.ERROR):
        conn = katsdpmetawriter.get_s3_connection(boto_dict)
    assert conn is None
    assert 'has no permissions' in caplog.text


def test_get_s3_connection_bad_host(s3_args, caplog):
    s3_args.s3_host = 'test.invalid'
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    with caplog.at_level(logging.ERROR):
        conn = katsdpmetawriter.get_s3_connection(boto_dict)
    assert conn is None
    assert 'Please check network and host address' in caplog.text


def test_get_s3_connection_fail_on_boto(s3_args, caplog):
    s3_args.secret_key = 'wrong'
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    with caplog.at_level(logging.ERROR):
        with pytest.raises(boto.exception.S3ResponseError):
            katsdpmetawriter.get_s3_connection(boto_dict, fail_on_boto=True)
    assert 'secret key is not valid' in caplog.text


def key_info(telstate, key):
    """Get all information about a telescope state key."""
    key_type = telstate.key_type(key)
    if key_type == katsdptelstate.KeyType.MUTABLE:
        value = telstate.get_range(key, st=0)
    else:
        value = telstate.get(key)
    return (key_type, value)


def test_write_rdb_local_lite(telstate, tmp_path_factory, mocker):
    path = tmp_path_factory.mktemp('dump') / 'dump.rdb'
    ctx = mocker.MagicMock()
    rate_bytes, key_errors = katsdpmetawriter._write_rdb(
        ctx, telstate, str(path), CBID, STREAM_NAME,
        boto_dict=None, key_name=None, lite=True
    )
    assert rate_bytes is None, 'rate_bytes does not apply with local-only dump'
    assert key_errors > 0      # We didn't fully populate dummy telstate
    ctx.inform.assert_called()

    telstate2 = katsdptelstate.TelescopeState()
    telstate2.load_from_file(path)
    for key in telstate.keys():
        if 'sdp_archived_streams' in key or 'another' in key:    # Not in the lite list
            assert key not in telstate2
        else:
            assert key_info(telstate, key) == key_info(telstate2, key)
    assert telstate2.get('capture_block_id') == CBID
    assert telstate2.get('stream_name') == STREAM_NAME


def test_write_rdb_s3_full(telstate, s3_args, tmp_path_factory, mocker):
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    path = tmp_path_factory.mktemp('dump') / 'dump.rdb.uploading'
    ctx = mocker.MagicMock()
    mocker.patch('katsdpmetawriter.timer', side_effect=[100.0, 105.0])
    rate_bytes, key_errors = katsdpmetawriter._write_rdb(
        ctx, telstate, str(path), CBID, STREAM_NAME,
        boto_dict=boto_dict, key_name='dump.rdb', lite=False
    )
    assert rate_bytes == path.stat().st_size / 5.0
    assert key_errors == 0, 'Should never be key errors with a full dump'
    ctx.inform.assert_called()

    s3_conn = boto.connect_s3(**boto_dict)
    bucket = s3_conn.get_bucket(CBID)
    obj_key = bucket.get_key('dump.rdb')
    assert obj_key is not None, 'Dump not found in S3'
    assert obj_key.get_contents_as_string() == path.read_bytes()

    telstate2 = katsdptelstate.TelescopeState()
    telstate2.load_from_file(path)
    for key in telstate.keys():
        assert key_info(telstate, key) == key_info(telstate2, key)
    assert telstate2.get('capture_block_id') == CBID
    assert telstate2.get('stream_name') == STREAM_NAME
    assert len(telstate2.keys()) == len(telstate.keys()) + 2


def test_write_rdb_zero_keys(telstate, tmp_path_factory, mocker, caplog):
    ctx = mocker.MagicMock()
    path = tmp_path_factory.mktemp('dump') / 'dump.rdb.uploading'
    telstate.clear()
    with caplog.at_level(logging.ERROR):
        rate_bytes, key_errors = katsdpmetawriter._write_rdb(
            ctx, telstate, str(path), CBID, STREAM_NAME,
            boto_dict=None, key_name=None, lite=False
        )
    assert rate_bytes is None
    assert key_errors == 0
    assert not path.exists()
    assert 'No valid telstate keys' in caplog.text


def test_write_rdb_lost_connection(telstate, tmp_path_factory, mocker, caplog):
    s3_args = argparse.Namespace(
        access_key=ADMIN_USER.access_key,
        secret_key=ADMIN_USER.secret_key,
        s3_host='test.invalid',
        s3_port=0
    )
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    mocker.patch('katsdpmetawriter.get_s3_connection', return_value=None)

    path = tmp_path_factory.mktemp('dump') / 'dump.rdb.uploading'
    ctx = mocker.MagicMock()
    with caplog.at_level(logging.ERROR):
        rate_bytes, key_errors = katsdpmetawriter._write_rdb(
            ctx, telstate, str(path), CBID, STREAM_NAME,
            boto_dict=boto_dict, key_name='dump.rdb', lite=False
        )
    assert rate_bytes is None
    assert 'Unable to store RDB dump' in caplog.text


def test_write_rdb_s3_permission_error(telstate, s3_args, tmp_path_factory, mocker, caplog):
    s3_args.access_key = READONLY_USER.access_key
    s3_args.secret_key = READONLY_USER.secret_key
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args)
    path = tmp_path_factory.mktemp('dump') / 'dump.rdb.uploading'
    ctx = mocker.MagicMock()
    with caplog.at_level(logging.ERROR):
        rate_bytes, key_errors = katsdpmetawriter._write_rdb(
            ctx, telstate, str(path), CBID, STREAM_NAME,
            boto_dict=boto_dict, key_name='dump.rdb', lite=False
        )
    assert rate_bytes is None
    assert 'does not have permission' in caplog.text


@pytest.fixture(params=[True, False])
async def device_server(request, s3_args, telstate, tmp_path_factory, event_loop):
    """Create and start a :class:`~.MetaWriterServer`.

    It is parametrized by whether to connect it to an S3 server.
    """
    rdb_path = tmp_path_factory.mktemp('dump')
    executor = concurrent.futures.ThreadPoolExecutor(1)
    boto_dict = katsdpmetawriter.make_boto_dict(s3_args) if request.param else None
    server = katsdpmetawriter.MetaWriterServer(
        '127.0.0.1', 0, event_loop, executor, boto_dict, str(rdb_path), telstate)
    await server.start()
    yield server
    await server.stop()


without_s3 = pytest.mark.parametrize('device_server', [False], indirect=True)
only_s3 = pytest.mark.parametrize('device_server', [True], indirect=True)


@pytest.fixture
async def device_client(device_server):
    address = device_server.server.sockets[0].getsockname()
    client = await aiokatcp.Client.connect(address[0], address[1])
    yield client
    client.close()
    await client.wait_closed()


async def get_sensor(client, sensor_name):
    """Get last sensor value (in encoded form), or None if no value."""
    reply, informs = await client.request('sensor-value', sensor_name)
    assert reply == [b'1']
    status = informs[0].arguments[3]
    value = informs[0].arguments[4]
    if status in {b'nominal', b'warning', b'error'}:
        return value
    else:
        return None


async def test_meta_write_lite_all(device_client):
    reply, informs = await device_client.request('write-meta', CBID)
    cs = f'{CBID}_{STREAM_NAME}'
    assert len(informs) == 3
    assert 'Starting write of lightweight metadata' in informs[0].arguments[0].decode()
    assert f'RDB extract and write for {cs} complete' in informs[1].arguments[0].decode()
    assert f'Lightweight meta-data for CB: {cs} written' in informs[2].arguments[0].decode()


async def test_meta_write_full_single(device_client, device_server, mocker):
    # There are two nested timers, so we have to return the start time twice
    # then the end time twice.
    mocker.patch('katsdpmetawriter.timer', side_effect=[100.0, 100.0, 105.0, 105.0])
    reply, informs = await device_client.request('write-meta', CBID, False, STREAM_NAME)
    cs = f'{CBID}_{STREAM_NAME}'
    assert len(informs) == 3
    assert 'Starting write of full metadata' in informs[0].arguments[0].decode()
    assert f'RDB extract and write for {cs} complete' in informs[1].arguments[0].decode()
    assert f'Full dump meta-data for CB: {cs} written' in informs[2].arguments[0].decode()

    s3 = device_server._boto_dict is not None
    if s3:
        s3_conn = boto.connect_s3(**device_server._boto_dict)
        bucket = s3_conn.get_bucket(CBID)
        obj_key = bucket.new_key(f'{cs}.full.rdb')
        size = len(obj_key.get_contents_as_string())
    else:
        path = pathlib.Path(device_server._rdb_path) / CBID / f'{cs}.full.rdb'
        size = path.stat().st_size

    assert await get_sensor(device_client, 'status') == b'idle'
    assert await get_sensor(device_client, 'last-write-stream') == STREAM_NAME.encode()
    assert await get_sensor(device_client, 'last-write-cbid') == CBID.encode()
    if device_server._boto_dict:
        assert float(await get_sensor(device_client, 'last-transfer-rate')) == size / 5.0
    else:
        assert await get_sensor(device_client, 'last-transfer-rate') is None


async def test_meta_write_too_many(device_client, mocker, event_loop):
    def slow_write_rdb(*args, **kwargs):
        result = orig_write_rdb(*args, **kwargs)
        unblock_write.wait()
        return result

    unblock_write = threading.Event()
    orig_write_rdb = katsdpmetawriter._write_rdb
    mocker.patch('katsdpmetawriter._write_rdb', slow_write_rdb)
    tasks = []
    # Start one more than the maximum number of concurrent writes
    for i in range(katsdpmetawriter.MAX_ASYNC_TASKS + 1):
        cbid = f'{CBID}{i}'
        tasks.append(
            event_loop.create_task(device_client.request('write-meta', cbid, True, STREAM_NAME))
        )
    unblock_write.set()
    await asyncio.gather(*tasks[:-1])       # Initial MAX_ASYNC_TASKS all succeed
    with pytest.raises(aiokatcp.FailReply, match='too many operations in progress'):
        await asyncio.gather(tasks[-1])


async def test_meta_writer_no_sdp_archived_streams(device_client, telstate):
    telstate.delete('sdp_archived_streams')
    with pytest.raises(aiokatcp.FailReply, match='cannot determine available streams'):
        await device_client.request('write-meta', CBID)


@without_s3
async def test_meta_writer_rename_failed(device_client, mocker):
    def mock_rename(src, dest):
        # Make os.rename fail by removing the source file.
        os.remove(src)
        return orig_rename(src, dest)

    orig_rename = os.rename
    mocker.patch('os.rename', mock_rename)
    with pytest.raises(aiokatcp.FailReply, match='Failed to store RDB dump'):
        await device_client.request('write-meta', CBID)


@only_s3
async def test_meta_writer_remove_failed(device_client, device_server, mocker, caplog):
    def mock_remove(filename):
        # Make os.remove fail by removing it twice
        orig_remove(filename)
        return orig_remove(filename)

    orig_remove = os.remove
    mocker.patch('os.remove', mock_remove)
    with caplog.at_level(logging.WARNING):
        await device_client.request('write-meta', CBID)
    assert 'Failed to remove transferred RDB file' in caplog.text

    # Make sure the S3 transfer still worked
    s3_conn = boto.connect_s3(**device_server._boto_dict)
    bucket = s3_conn.get_bucket(CBID)
    obj_key = bucket.new_key(f'{CBID}_{STREAM_NAME}.rdb')
    size = len(obj_key.get_contents_as_string())
    assert size > 0
