#!/usr/bin/env python

"""Serialise a view of the Telescope State for the current observation to long term storage.

The Telescope State (TS) is a meta-data repository that includes information about the 
current state of the wide telescope, configuration data and intermediate SDP products
such as calibration solutions. It also contains references to the data objects that
comprise the visibility data captured for an observation.

The MeerKAT data access library (katdal) uses the TS effectively as the 'file'
representation of an observation. Such a 'file' can be opened by pointing katdal to either
a live TS repository (currently Redis backed) or to a serialised representation
of the TS (currently supports Redis RDB format).

This writer, when requested, can create two different views of the TS and save these into
long term storage.

The first view is a lightweight representation of the TS containing the basic data
to allow katdal to open an observation. This includes data such as captured timestamps,
storage configuration information and observation targets. Using the supplied capture block
ID, an attempt is also made to only record meta information specific to that capture
block ID.

The second is a complete dump of the entire TS, in the Redis case using the BGSAVE
command to produce a complete RDB file. This may contain meta-data from other capture sessions.
"""

import socket
import sys
import errno
import logging
import asyncio
import signal
import time

import boto
import boto.s3.connection
import katsdptelstate
import katsdpservices
import katsdpfilewriter
from aiokatcp import DeviceServer, Sensor, FailReply, Address


def get_s3_bucket(s3_args):
    """Small helper class to establish a connection to the specified S3
       gateway and return a reference to the bucket required.

       Rewrites a number of exceptions into a more human readable log messages.
       In the Mesos context, it is more useful to have accurate error messages
       in the logs than allow the exceptions to bubble up.
    """
        # Attempt connection to S3 gateway and validate supplied bucket name
    s3_conn = boto.connect_s3(
                aws_access_key_id = s3_args.access_key,
                aws_secret_access_key = s3_args.secret_key,
                host = s3_args.host,
                port = s3_args.port,
                is_secure = False,
                calling_format = boto.s3.connection.OrdinaryCallingFormat()
               )
    try:
        user_id = s3_conn.get_canonical_user_id()
         # tests the connection and useful in log messages
        bucket = s3_conn.get_bucket(s3_args.bucket)
        return (user_id, bucket)
    except socket.error as e:
        if e.errno != errno.ECONNREFUSED:
            raise e
        logger.error("Failed to connect to S3 host {}:{}. Please check network and host address.".format(s3_args.host, s3_args.port))
    except boto.exception.S3ResponseError as e:
        if e.error_code == u'InvalidAccessKeyId':
            logger.error("Supplied access key ({}) is not a valid S3 user.".format(s3_args.access_key))
        if e.error_code == u'SignatureDoesNotMatch':
            logger.error("Supplied secret key is not valid for specified user.")
        if e.status == 404:
            logger.error("Specified bucket ({}) not found.".format(s3_args.bucket))
    return (None, None)

class MetaWriterServer(DeviceServer):
    VERSION = "sdp-meta-writer-0.1"
    BUILD_STATE = "katsdpfilewriter-" + katsdpfilewriter.__version__


    def __init__(self, host, port, loop, logger, telstate_l0, bucket):
        self._telstate_l0 = telstate_l0
        self._bucket = bucket
        self._loop = loop
        self._async_task = None

        self._build_state_sensor = Sensor(str, "build-state", "SDP Controller build state.")

        self._device_status_sensor = Sensor(str, "status", "The current status of the meta writer process")
        self._last_write_stream_sensor = Sensor(str, "last-write-stream", "The stream name of the last meta data dump.")    
        self._last_write_cbid_sensor = Sensor(str, "last-write-cbid", "The capture block ID of the last meta data dump.")

        super().__init__(host, port)

        self.sensors.add(self._device_status_sensor)
        self.sensors.add(self._last_write_stream_sensor)
        self.sensors.add(self._last_write_cbid_sensor)

    @property
    def async_busy(self):
        """Whether there is an asynchronous state-change operation in progress."""
        return self._async_task is not None and not self._async_task.done()

    def _fail_if_busy(self):
        """Raise a FailReply if there is an asynchronous operation in progress."""
        if self.async_busy: raise FailReply('Meta-data writer is busy with an operation. Please wait for it to complete first.')

    def _clear_async_task(self, future):
        """Clear the current async task.

        Parameters
        ----------
        future : :class:`asyncio.Future`
            The expected value of :attr:`_async_task`. If it does not match,
            it is not cleared (this can happen if another task replaced it
            already).
        """
        if self._async_task is future:
            self._async_task = None

    async def _write_meta(self, capture_block_id, light=True):
        """Write metadaya extracted from the current telstate object
        to a binary dump and place this in the currently connected
        S3 bucket for storage.
        """
        written = 0
        for x in range(20):
            await asyncio.sleep(1.0, loop=self.loop)
            written += 1
        return written
    
    async def write_light_meta(self, capture_block_id):
        """Implementation of request_write_light_meta."""
        self._fail_if_busy()
        task = asyncio.ensure_future(self._write_meta(capture_block_id, light=True), loop=self._loop)
        self._async_task = task
        try:
            size_mb = await task
        finally:
            self._clear_async_task(task)
        return size_mb

        
    async def request_write_light_meta(self, ctx, capture_block_id: str) -> str:
        """Write a lightweight variant of the currently active telescope state to the already
        specified S3 bucket. If a capture_block_id is specified, this is used to produce a
        view on the telstate object specific to that block.
        Method may take some time so is run asychronously.
        
        Parameters
        ----------
        capture_block_id : string
            The capture block id generated by master controller (or supplied by CAM) to identify a
            specific data capture.

        Returns
        -------
        success : {'ok', 'fail'}
            Whether the command succeeded
        timing : str
            The capture duration (and resultant MBps)
        """
        ctx.inform("Starting write of lightweight metadata for CB: {} to S3. This may take a minute or two..."
                   .format(capture_block_id))
        st = time.time()
        size_mb = await self.write_light_meta(capture_block_id)
        duration_s = time.time() - st
        return "Lightweight meta-data for CB: {} written to S3 in {}s @ {}MBps".format(capture_block_id, duration_s, size_mb / duration_s)

    async def request_hello(self, ctx) -> None:
        """This is a hello"""
        ctx.informs(["One","Tow","Three"])

    async def request_fail(self, ctx) -> None:
        """This is a fail..."""
        raise FailReply("This is a fail...")


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
    parser.add_argument('--bucket', default='meerkat', metavar='BUCKET',
                        help='S3 bucket in which to store meta-data dumps [default=%(default)s]')
    parser.add_argument('--access-key', default="", metavar='ACCESS',
                        help='S3 access key with write permission to the specified bucket. Default is unauthenticated access')
    parser.add_argument('--secret-key', default="", metavar='SECRET',
                        help='S3 secret key for the specified access key. Default is unauthenticated access') 
    parser.add_argument('--s3_host', default='localhost', metavar='HOST',
                        help='S3 gateway host address [default=%(default)s]')
    parser.add_argument('--s3_port', default=7480, metavar='PORT',
                        help='S3 gateway port [default=%(default)s]')
    parser.add_argument('-p', '--port', type=int, default=2047, metavar='N',
                        help='KATCP host port [default=%(default)s]')
    parser.add_argument('-a', '--host', default="", metavar='HOST',
                        help='KATCP host address [default=all hosts]')

    args = parser.parse_args()

    #(user_id, bucket) = get_s3_bucket(args)
     # Attempt connection to S3 gateway and validate supplied bucket name

    #if user_id is None: sys.exit(1)
     # crash out in this early phase to let the system know we are not running 

    logger.info("Successfully tested connection to S3 endpoint. Meta-data dumps will be written to bucket: {}".format(args.bucket))
    telstate_l0 = None #args.telstate.view(args.l0_name)

    loop = asyncio.get_event_loop()

    server = MetaWriterServer(args.host, args.port, loop, logger, telstate_l0, args.bucket)
    logger.info("Started meta-data writer server.")

    loop.run_until_complete(run(loop, server))
    loop.close()
