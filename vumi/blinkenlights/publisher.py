
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Publisher
from vumi.message import Message

from twisted.internet.task import LoopingCall

import time
import os
import socket

class HeartBeatMessage(Message):
    """
    Basically just a wrapper around a dict for now
    """

    def __init__(self, **kw):
        super(HeartBeatMessage, self).__init__(self, **kw)


class HeartBeatPublisher(Publisher):
    """
    A publisher which send periodic heartbeat messages to the AMQP heartbeat.inbound queue
    """

    HEARTBEAT_PERIOD_SECS = 10

    def __init__(self, system_id, worker_id):
        self.system_id = system_id
        self.worker_id = worker_id
        self.routing_key = "heartbeat.inbound"
        self.durable     = True
        self._task       = None

    def _tick(self):
        msg = self.create_msg()
        self.publish_message(msg)
        log.msg("Sent message: %s" % msg)

    def create_msg(self):
        attrs = {
            'system_id' : self.system_id,
            'worker_id' : self.worker_id,
            'hostname'  : socket.gethostname(),
            'timestamp' : time.time(),
            'pid'       : os.getpid(),
        }
        return HeartBeatMessage(**attrs)

    def start(self, channel):
        super(HeartBeatPublisher, self).start(channel)
        self._task = LoopingCall(self._tick)
        done = self._task.start(HeartBeatPublisher.HEARTBEAT_PERIOD_SECS, now=False)
        done.addErrback(lambda failure: log.err(failure, "HeartBeatPublisher task died"))

    def stop(self):
        """Stop publishing metrics."""
        if self._task:
            self._task.stop()
            self._task = None
