# -*- test-case-name: vumi.blinkenlights.heartbeat.tests.test_metadata -*-


class HeartBeatMetadata(object):
    """
    Base class for metadata production.

    Specialized worker classes should subclass this,
    in order to include class-specific metadata in messages
    """

    TYPE_NAME = "undefined"

    def __init__(self, worker):
        self.worker = worker

    def produce(self):
        """Produce metadata"""
        return {
            'type': self.TYPE_NAME
        }
