################################################################################
# Copyright (c) 2018-2020, National Research Foundation (SARAO)
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

"""Serialise a view of the Telescope State for the current observation to long term storage.

The Telescope State (TS) is a meta-data repository that includes information about the
current state of the wider telescope, configuration data and intermediate SDP products
such as calibration solutions. It also contains references to the data objects that
comprise the visibility data captured for an observation.

The MeerKAT data access library (katdal) uses the TS effectively as the 'file'
representation of an observation. Such a 'file' can be opened by pointing katdal to either
a live TS repository (currently Redis-backed) or to a serialised representation
of the TS (currently supports Redis RDB format).

This writer, when requested, can create two different views of the TS and save these into
long-term storage.

The first view is a lightweight representation of the TS containing the basic data
to allow katdal to open an observation. This includes data such as captured timestamps,
storage configuration information and observation targets. Using the supplied capture block
ID, an attempt is also made to only record meta information specific to that capture
block ID.

The second is a complete dump of the entire TS. This may contain meta-data from
other capture sessions.
"""

import logging
import os
import socket
import asyncio
import time
import enum
import pathlib
from collections import deque

# async_generator provides a backport of Python 3.7's asynccontextmanager
from async_generator import asynccontextmanager
import aiobotocore.config
import botocore.exceptions
import katsdptelstate
from katsdptelstate.aio.rdb_writer import RDBWriter
from aiokatcp import DeviceServer, Sensor, FailReply


# BEGIN VERSION CHECK
# Get package version when locally imported from repo or via -e develop install
try:
    import katversion as _katversion
except ImportError:
    import time as _time
    __version__ = "0.0+unknown.{}".format(_time.strftime('%Y%m%d%H%M'))
else:
    __version__ = _katversion.get_version(__path__[0])    # type: ignore
# END VERSION CHECK


logger = logging.getLogger(__name__)

# Fairly arbitrary limit on number of concurrent meta data writes
# that we allow. Tradeoff between not stopping observations and
# taking too long to discover some blocking fault.
MAX_ASYNC_TASKS = 10

# Template of key names that we would like to preserve when dumping
# a lite version of Telstate. Since we always back observations with
# a full dump of Telstate, we don't fail on missing entries, but do
# log them.
# Each string entry is format'ed with the following substitutions:
#
#    {cb}: The capture block ID which uniquely identifies this data capture
#    {sn}: The name of a specific stream in the capture (e.g. sdp_l0)
#    m???: A special that represents a glob-style pattern match used to
#          do wildcard MeerKAT antenna name matching for certain keys.
#    s????: A special that represents a glob-style pattern match used to
#          do wildcard SKA prototype antenna name matching for certain keys.
#
LITE_KEYS = [
    "{sn}_int_time",
    "{sn}_sync_time",
    "{sn}_bls_ordering",
    "{sn}_n_chans",
    "{sn}_bandwidth",
    "{sn}_center_freq",
    "{sn}_s3_endpoint_url",
    "{sn}_stream_type",
    "{sn}_need_weights_power_scale",
    "{cb}_obs_params",
    "{cb}_obs_script_log",
    "{cb}_obs_label",
    "{cb}_obs_activity",
    "{cb}_{sn}_chunk_info",
    "{cb}_{sn}_first_timestamp",
    "sub_pool_resources",
    "sub_band",
    "sub_product",
    "m???_observer",
    "m???_activity",
    "m???_target",
    "s????_observer",
    "s????_activity",
    "s????_target",
    "e???_observer",
    "e???_activity",
    "e???_target",
    "cbf_target",
    "sdp_config"
]


def timer():
    """Get timestamp for measuring elapsed time.

    This is a wrapper that's made so that it can be mocked easily.
    """
    return time.monotonic()


def make_botocore_dict(s3_args):
    """Create a dict suitable for passing into aiobotocore using the supplied args."""
    return {
        "aws_access_key_id": s3_args.access_key,
        "aws_secret_access_key": s3_args.secret_key,
        "endpoint_url": f"http://{s3_args.s3_host}:{s3_args.s3_port}",
        "use_ssl": False,
        "config": aiobotocore.config.AioConfig(s3={"addressing_style": "path"})
    }


async def get_lite_keys(telstate, capture_block_id, stream_name):
    """Uses capture_block_id and stream_name, along with the template
    of keys to store in the lite dump, to build a full list of the keys
    to be dumped.
    Note: We avoid using telstate views here since we want to write fully
    qualified keys into the lite database to easily allow merge later on.
    The philosophy of the lite dump is to change as little as possible.
    """
    keys = []
    for key in LITE_KEYS:
        if key.find('?') >= 0:
            keys.extend(await telstate.keys(filter=key))
        else:
            keys.append(key.format(cb=capture_block_id, sn=stream_name))
    return keys


@asynccontextmanager
async def get_s3_connection(botocore_dict, fail_on_boto=False):
    """Test the connection to S3 as described in the args.

    Return the connection object.

    In general we are more concerned with informing the user why the
    connection failed, rather than raising exceptions. Users should always
    check the return value and make appropriate decisions.

    If set, fail_on_boto will not suppress boto exceptions. Used when verifying
    credentials.

    Returns
    -------
    s3_conn : S3Connection
        A connection to the s3 endpoint. None if a connection error occurred.
    """
    session = aiobotocore.session.get_session()
    try:
        # reliable way to test connection and access keys
        async with session.create_client('s3', **botocore_dict) as s3_conn:
            await s3_conn.list_buckets()
            yield s3_conn
            return
    except socket.error as e:
        logger.error(
            "Failed to connect to S3 host %s. Please check network and host address. (%s)",
            botocore_dict['endpoint_url'], e)
    except botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code')
        if error_code == 'InvalidAccessKeyId':
            logger.error(
                "Supplied access key %s is not a valid S3 user.",
                botocore_dict['aws_access_key_id'])
        elif error_code == 'SignatureDoesNotMatch':
            logger.error("Supplied secret key is not valid for specified user.")
        elif error_code == 'AccessDenied':
            logger.error(
                "Supplied access key (%s) has no permissions on this server.",
                botocore_dict['aws_access_key_id'])
        else:
            logger.error(e)
        if fail_on_boto:
            raise
    yield None


async def _write_rdb(ctx, telstate, dump_filename, capture_block_id, stream_name,
                     botocore_dict, key_name, lite=True):
    """Synchronous code used to create an on-disk dump.

    If a `botocore_dict` is supplied, upload the dump to S3, with the name
    `key_name`.
    """
    keys = None
    if lite:
        keys = await get_lite_keys(telstate, capture_block_id, stream_name)
    logger.info(
        "Writing %s keys to local RDB dump %s",
        str(len(keys)) if lite else "all", dump_filename)

    supplemental_telstate = katsdptelstate.aio.TelescopeState()
    await supplemental_telstate.set('stream_name', stream_name)
    await supplemental_telstate.set('capture_block_id', capture_block_id)
    with RDBWriter(dump_filename) as rdbw:
        await rdbw.save(telstate, keys)
        if rdbw.keys_written > 0:
            await rdbw.save(supplemental_telstate)
    key_errors = rdbw.keys_failed
    if not rdbw.keys_written:
        logger.error("No valid telstate keys found for %s_%s", capture_block_id, stream_name)
        return (None, key_errors)
    logger.info("Write complete. %s errors", key_errors)
    ctx.inform(
        "RDB extract and write for {}_{} complete. {} errors"
        .format(capture_block_id, stream_name, key_errors))

    if not botocore_dict:
        return (None, key_errors)

    async with get_s3_connection(botocore_dict) as s3_conn:
        if not s3_conn:
            logger.error("Unable to store RDB dump in S3.")
            return (None, key_errors)
        file_size = os.path.getsize(dump_filename)
        rate_bytes = 0
        written_bytes = 0
        try:
            await s3_conn.create_bucket(Bucket=capture_block_id)
            st = timer()
            with open(dump_filename, 'rb') as dump_data:
                await s3_conn.put_object(
                    Bucket=capture_block_id,
                    Key=key_name,
                    Body=dump_data)
                written_bytes = dump_data.tell()
            rate_bytes = written_bytes / (timer() - st)
        except botocore.exceptions.ClientError as e:
            status = e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')
            if status in {403, 409}:
                logger.error(
                    "Unable to store RDB dump as access key %s "
                    "does not have permission to write to bucket %s",
                    botocore_dict["aws_access_key_id"], capture_block_id)
                return (None, key_errors)
            elif status == 404:
                logger.error(
                    "Unable to store RDB dump as the bucket %s or key %s has been lost.",
                    capture_block_id, key_name)
                return (None, key_errors)
            else:
                logger.error(
                    "Error writing to %s/%s in S3",
                    capture_block_id, key_name, exc_info=True)
                return (None, key_errors)
        if written_bytes != file_size:
            logger.error(
                "Incorrect number of bytes written (%d/%d) when writing RDB dump %s",
                written_bytes, file_size, dump_filename)
            return (None, key_errors)
        return (rate_bytes, key_errors)


class DeviceStatus(enum.Enum):
    IDLE = 1
    QUEUED = 2


class MetaWriterServer(DeviceServer):
    VERSION = "sdp-meta-writer-0.1"
    BUILD_STATE = "katsdpmetawriter-" + __version__

    def __init__(self, host, port, botocore_dict, rdb_path, telstate):
        self._botocore_dict = botocore_dict
        self._async_tasks = deque()
        self._rdb_path = rdb_path
        self._telstate = telstate

        self._build_state_sensor = Sensor(str, "build-state", "SDP Controller build state.")

        self._device_status_sensor = Sensor(
            DeviceStatus, "status", "The current status of the meta writer process")
        self._last_write_stream_sensor = Sensor(
            str, "last-write-stream", "The stream name of the last meta data dump.")
        self._last_write_cbid_sensor = Sensor(
            str, "last-write-cbid", "The capture block ID of the last meta data dump.")
        self._key_failures_sensor = Sensor(
            int, "key-failures",
            "Count of the number of failures to write a desired key to the RDB dump. "
            "(prometheus: counter)")
        self._last_transfer_rate = Sensor(
            float, "last-transfer-rate",
            "Rate of last data transfer to S3 endpoint in Bps. (prometheus: gauge)")
        self._last_dump_duration = Sensor(
            float, "last-dump-duration",
            "Time taken to write the last dump to disk. (prometheus: gauge)", "s")

        super().__init__(host, port)

        self._build_state_sensor.set_value(self.BUILD_STATE)
        self.sensors.add(self._build_state_sensor)
        self._device_status_sensor.set_value(DeviceStatus.IDLE)
        self.sensors.add(self._device_status_sensor)
        self.sensors.add(self._last_write_stream_sensor)
        self.sensors.add(self._last_write_cbid_sensor)
        self.sensors.add(self._last_transfer_rate)
        self.sensors.add(self._last_dump_duration)
        self._key_failures_sensor.set_value(0)
        self.sensors.add(self._key_failures_sensor)

    def _fail_if_busy(self):
        """Raise a FailReply if there are too many asynchronous operations in progress."""
        busy_tasks = 0
        for task in self._async_tasks:
            if not task.done():
                busy_tasks += 1
        if busy_tasks >= MAX_ASYNC_TASKS:
            raise FailReply(
                ('Meta-data writer has too many operations in progress (max {}). '
                 'Please wait for one to complete first.').format(MAX_ASYNC_TASKS))

    def _clear_async_task(self, future):
        """Clear the specified async task.

        Parameters
        ----------
        future : :class:`asyncio.Future`
            The expected value of :attr:`_async_task`.
        """
        try:
            self._async_tasks.remove(future)
        except IndexError:
            pass
        if not self._async_tasks:
            self._device_status_sensor.set_value(DeviceStatus.IDLE)

    async def _write_meta(self, ctx, capture_block_id, stream_name, lite=True):
        """Write meta-data extracted from the current telstate object
        to a binary dump and place this in the currently connected
        S3 bucket for storage.
        """
        additional_name = "full." if not lite else ""
        dump_folder = os.path.join(self._rdb_path, capture_block_id)
        os.makedirs(dump_folder, exist_ok=True)
        basename = "{}_{}.{}rdb".format(capture_block_id, stream_name, additional_name)
        dump_filename = os.path.join(dump_folder, basename + '.uploading')
        st = timer()
        # Generate local RDB dump and write into S3 - note that
        # capture_block_id is used as the bucket name for storing meta-data
        # regardless of the stream selected.
        # The full capture_block_stream_name is used as the bucket for payload
        # data for the particular stream.
        (rate_b, key_errors) = await _write_rdb(
            ctx, self._telstate, dump_filename,
            capture_block_id, stream_name, self._botocore_dict, basename, lite)
        et = timer()
        sensor_timestamp = time.time()
        self._last_write_stream_sensor.set_value(stream_name, timestamp=sensor_timestamp)
        self._last_write_cbid_sensor.set_value(capture_block_id, timestamp=sensor_timestamp)
        self._last_dump_duration.set_value(et - st, timestamp=sensor_timestamp)
        if key_errors > 0:
            self._key_failures_sensor.set_value(
                self._key_failures_sensor.value + key_errors,
                Sensor.Status.ERROR)

        if not rate_b:
            try:
                trawler_filename = os.path.join(dump_folder, basename)
                # Prepare to rename file so that the trawler process can
                # attempt the S3 upload at a later date.
                os.rename(dump_filename, trawler_filename)
            except FileNotFoundError:
                msg = (
                    "Failed to store RDB dump, and couldn't find file to rename. "
                    "This error cannot be recovered from."
                )
                logger.error(msg)
                raise FailReply(msg)
        else:
            logger.info(
                "RDB file written to bucket %s with key %s",
                capture_block_id, os.path.basename(dump_filename))
            try:
                os.remove(dump_filename)
            except Exception as e:
                # it won't interfere with the trawler so we just continue
                logger.warning("Failed to remove transferred RDB file %s. (%s)", dump_filename, e)
        return rate_b

    async def write_meta(self, ctx, capture_block_id, streams, lite=True):
        """Implementation of request_write_meta."""
        rate_per_stream = {}
        for stream in streams:
            task = asyncio.ensure_future(
                self._write_meta(ctx, capture_block_id, stream, lite))
            self._device_status_sensor.set_value(DeviceStatus.QUEUED)
            # we risk queue depth expansion at this point, but we are really
            # only checking to prevent outrageous failures.
            self._async_tasks.append(task)
            try:
                rate_b = await task
            finally:
                self._clear_async_task(task)
            rate_per_stream[stream] = rate_b

        dump_folder = os.path.join(self._rdb_path, capture_block_id)
        if not lite and os.path.exists(dump_folder):
            # We treat writing the streams for a full meta dump as the
            # completion of meta data for that particular capture block id
            # (assuming at least one stream was written).
            touch_file = os.path.join(dump_folder, "complete")
            pathlib.Path(touch_file).touch(exist_ok=True)

        return rate_per_stream

    async def request_write_meta(
            self, ctx, capture_block_id: str, lite: bool = True, stream_name: str = None) -> None:
        """Write a dump of a subset of currently active telescope state to disk and
        optionally archive it to the preconfigured S3 endpoint. The precise subset
        is controlled through the selection of capture_block_id, stream_name and
        the lite boolean.
        Method may take some time so is run asynchronously.

        Parameters
        ----------
        capture_block_id : string
            The capture block id generated by master controller to identify a
            specific data capture. Typically this will be an integer representing the
            start time of the observation in epoch seconds (+/- to allow for
            uniqueness if required).
        lite : bool, optional
            If True then a very limited subset of telstate keys are written to the dump,
            otherwise a 'full' dump is produced. Currently 'full' is the entire telescope
            state database, but in the future may be restricted to meta-data relevant
            only to the chosen capture_block_id and stream_name.
        stream_name : string, optional
            The specific stream name to use in extracting stream specific meta-data.
            (e.g. sdp_l0) If no stream is specified, all sdp.vis streams with
            attached writers will be saved individually.
        """
        self._fail_if_busy()
        if not stream_name:
            streams = await self._telstate.get('sdp_archived_streams')
            if not streams:
                raise FailReply(
                    "No stream specified, and cannot determine available streams from telstate.")
            streams = [stream for stream in streams
                       if await self._telstate.view(stream).get('stream_type') == 'sdp.vis']
        else:
            streams = [stream_name]

        ctx.inform(
            ("Starting write of {} metadata for CB: {} and Streams: {} to S3. "
             "This may take a minute or two...")
            .format("lightweight" if lite else "full", capture_block_id, streams))
        rate_per_stream = await self.write_meta(ctx, capture_block_id, streams, lite)
        peak_rate = 0
        dump_type_name = "Lightweight" if lite else "Full dump"
        for stream, rate_b in rate_per_stream.items():
            if not rate_b:
                ctx.inform(
                    "{} meta-data for CB: {}_{} written to local disk only"
                    .format(dump_type_name, capture_block_id, stream))
            else:
                ctx.inform(
                    "{} meta-data for CB: {}_{} written to S3 @ {:.2f}MBps"
                    .format(dump_type_name, capture_block_id, stream, rate_b / 1e6))
                peak_rate = max(peak_rate, rate_b)
        if peak_rate > 0:
            self._last_transfer_rate.set_value(peak_rate)
