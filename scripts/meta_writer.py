#!/usr/bin/env python3

import os
import sys
import logging
import asyncio
import signal
from concurrent.futures import ThreadPoolExecutor

import katsdpservices
import katsdpmetawriter


def on_shutdown(loop, server):
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)
     # in case the exit code below borks, we allow shutdown via traditional means
    server.halt()


async def run(loop, server):
    await server.start()
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, lambda: on_shutdown(loop, server))
    await server.join()


if __name__ == '__main__':
    katsdpservices.setup_logging()
    logger = logging.getLogger("katsdpmetawriter")
    katsdpservices.setup_restart()

    parser = katsdpservices.ArgumentParser()
    parser.add_argument('--rdb-path', default="/var/kat/data", metavar='RDBPATH',
                        help='Root in which to write RDB dumps.')
    parser.add_argument('--store-s3', dest='store_s3', default=False, action='store_true',
                        help='Enable storage of RDB dumps in S3')
    parser.add_argument('--access-key', default="", metavar='ACCESS',
                        help='S3 access key with write permission to the specified bucket. Default is unauthenticated access')
    parser.add_argument('--secret-key', default="", metavar='SECRET',
                        help='S3 secret key for the specified access key. Default is unauthenticated access')
    parser.add_argument('--s3-host', default='localhost', metavar='HOST',
                        help='S3 gateway host address [default=%(default)s]')
    parser.add_argument('--s3-port', default=7480, metavar='PORT',
                        help='S3 gateway port [default=%(default)s]')
    parser.add_argument('-p', '--port', type=int, default=2049, metavar='N',
                        help='KATCP host port [default=%(default)s]')
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='KATCP host address [default=all hosts]')

    args = parser.parse_args()

    if not os.path.exists(args.rdb_path):
        logger.error("Specified RDB path, %s, does not exist.", args.rdb_path)
        sys.exit(2)

    boto_dict = None
    if args.store_s3:
        boto_dict = katsdpmetawriter.make_boto_dict(args)
        s3_conn = katsdpmetawriter.get_s3_connection(boto_dict, fail_on_boto=True)
        if s3_conn:
            user_id = s3_conn.get_canonical_user_id()
            s3_conn.close()
             # we rebuild the connection each time we want to write a meta-data dump
            logger.info("Successfully tested connection to S3 endpoint as %s.", user_id)
        else:
            logger.warning("S3 endpoint %s:%s not available. Files will only be written locally.", args.s3_host, args.s3_port)
    else:
        logger.info("Running in disk only mode. RDB dumps will not be written to S3")

    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(3)

    server = katsdpmetawriter.MetaWriterServer(args.host, args.port, loop, executor, boto_dict, args.rdb_path, args.telstate)
    logger.info("Started meta-data writer server.")

    loop.run_until_complete(run(loop, server))
    executor.shutdown()
    loop.close()
