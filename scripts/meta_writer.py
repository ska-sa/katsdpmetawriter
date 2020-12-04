#!/usr/bin/env python3

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

import os
import sys
import logging
import asyncio
import signal

import aioredis
import katsdptelstate.aio.redis
import katsdpservices
import katsdpmetawriter


def on_shutdown(loop, server):
    # in case the exit code below borks, we allow shutdown via traditional means
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
    server.halt()


async def get_async_telstate(endpoint: katsdptelstate.endpoint.Endpoint):
    client = await aioredis.create_redis_pool(f'redis://{endpoint.host}:{endpoint.port}')
    return katsdptelstate.aio.TelescopeState(katsdptelstate.aio.redis.RedisBackend(client))


async def main():
    katsdpservices.setup_logging()
    logger = logging.getLogger("katsdpmetawriter")
    katsdpservices.setup_restart()

    parser = katsdpservices.ArgumentParser()
    parser.add_argument(
        '--rdb-path', default="/var/kat/data", metavar='RDBPATH',
        help='Root in which to write RDB dumps')
    parser.add_argument(
        '--store-s3', dest='store_s3', default=False, action='store_true',
        help='Enable storage of RDB dumps in S3')
    parser.add_argument(
        '--access-key', default="", metavar='ACCESS',
        help='S3 access key with write permission to the specified bucket [unauthenticated]')
    parser.add_argument(
        '--secret-key', default="", metavar='SECRET',
        help='S3 secret key for the specified access key [unauthenticated]')
    parser.add_argument(
        '--s3-host', default='localhost', metavar='HOST',
        help='S3 gateway host address [%(default)s]')
    parser.add_argument(
        '--s3-port', default=7480, metavar='PORT',
        help='S3 gateway port [%(default)s]')
    parser.add_argument(
        '-p', '--port', type=int, default=2049, metavar='N',
        help='KATCP host port [%(default)s]')
    parser.add_argument(
        '-a', '--host', default="", metavar='HOST',
        help='KATCP host address [all hosts]')

    args = parser.parse_args()

    if not os.path.exists(args.rdb_path):
        logger.error("Specified RDB path, %s, does not exist.", args.rdb_path)
        sys.exit(2)

    botocore_dict = None
    if args.store_s3:
        botocore_dict = katsdpmetawriter.make_botocore_dict(args)
        async with katsdpmetawriter.get_s3_connection(botocore_dict, fail_on_boto=True) as s3_conn:
            if s3_conn:
                # we rebuild the connection each time we want to write a meta-data dump
                logger.info("Successfully tested connection to S3 endpoint.")
            else:
                logger.warning(
                    "S3 endpoint %s:%s not available. Files will only be written locally.",
                    args.s3_host, args.s3_port)
    else:
        logger.info("Running in disk only mode. RDB dumps will not be written to S3")

    telstate = await get_async_telstate(args.telstate_endpoint)
    server = katsdpmetawriter.MetaWriterServer(
        args.host, args.port, botocore_dict, args.rdb_path, telstate)
    logger.info("Started meta-data writer server.")
    loop = asyncio.get_event_loop()
    await server.start()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, lambda: on_shutdown(loop, server))
    await server.join()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
