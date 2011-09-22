from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial import unittest

from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.tests.utils import get_stubbed_worker
from vumi.transports.base import Transport


class TransportTestCase(unittest.TestCase):
    """
    This is a base class for testing transports.
    """

    transport_name = "sphex"
    transport_class = None

    def setUp(self):
        self._workers = []
        self._amqp = FakeAMQPBroker()

    def tearDown(self):
        for worker in self._workers:
            worker.stopWorker()

    @inlineCallbacks
    def get_transport(self, config, cls=None, start=True):
        """
        Get an instance of a transport class.

        :param config: Config dict.
        :param cls: The transport class to instantiate.
                    Defaults to :attr:`transport_class`
        :param start: True to start the transport (default), False otherwise.

        Some default config values are helpfully provided in the
        interests of reducing boilerplate:

        * ``transport_name`` defaults to :attr:`self.transport_name`
        """

        if cls is None:
            cls = self.transport_class
        config.setdefault('transport_name', self.transport_name)
        worker = get_stubbed_worker(cls, config, self._amqp)
        self._workers.append(worker)
        if start:
            yield worker.startWorker()
        returnValue(worker)

    def assert_rkey_attr(self, rkey_suffix, obj, tr_name=None):
        if tr_name is None:
            tr_name = self.transport_name
        self.assertEqual("%s.%s" % (tr_name, rkey_suffix), obj.routing_key)

    def assert_basic_rkeys(self, transport):
        self.assert_rkey_attr('events', transport.event_publisher)
        self.assert_rkey_attr('inbound', transport.message_publisher)
        self.assert_rkey_attr('failures', transport.failure_publisher)
        self.assert_rkey_attr('outbound', transport.message_consumer)


class BaseTransportTestCase(TransportTestCase):
    transport_name = 'carrier_pigeon'
    transport_class = Transport

    @inlineCallbacks
    def test_start_transport(self):
        tr = yield self.get_transport({})
        self.assertEqual(self.transport_name, tr.transport_name)
        self.assert_basic_rkeys(tr)
