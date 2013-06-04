
"""
In the spirit of reducing complexity inside the main worker classes,
we farm out the production of metadata to these helper classes.
"""

from vumi.application.base import ApplicationWorker
from vumi.transports.base import Transport
from vumi.dispatchers.endpoint_dispatchers import Dispatcher


class HeartBeatMetadata(object):

    TYPE_NAME = "undefined"

    def __init__(self, worker):
        self.worker = worker

    @classmethod
    def create_producer(cls, worker):
        """Find an appropriate metadata producer"""
        if isinstance(worker, ApplicationWorker):
            return ApplicationMetadata(worker)
        elif isinstance(worker, Transport):
            return TransportMetadata(worker)
        elif isinstance(worker, Dispatcher):
            return DispatcherMetadata(worker)
        else:
            return GenericMetadata(worker)

    def produce(self):
        """Produce metadata"""
        return {
            'type': self.TYPE_NAME
        }


class GenericMetadata(HeartBeatMetadata):
    TYPE_NAME = "generic"


class TransportMetadata(HeartBeatMetadata):
    TYPE_NAME = "transport"


class DispatcherMetadata(HeartBeatMetadata):
    TYPE_NAME = "transport"


class ApplicationMetadata(HeartBeatMetadata):
    TYPE_NAME = "transport"
