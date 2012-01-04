from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial import unittest

from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.tests.utils import get_stubbed_worker, UTCNearNow, RegexMatcher
from vumi.message import TransportUserMessage, TransportEvent
from vumi.transports.base import Transport
from vumi.transports.failures import FailureMessage


class TransportTestCase(unittest.TestCase):
    """
    This is a base class for testing transports.

    Not to be confused with BaseTransportTestCase below.
    """

    transport_name = "sphex"
    transport_type = None
    transport_class = None

    def setUp(self):
        self._workers = []
        self._amqp = FakeAMQPBroker()

    @inlineCallbacks
    def tearDown(self):
        for worker in self._workers:
            yield worker.stopWorker()

    def rkey(self, name):
        return "%s.%s" % (self.transport_name, name)

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
        self.assert_rkey_attr('event', transport.event_publisher)
        self.assert_rkey_attr('inbound', transport.message_publisher)
        self.assert_rkey_attr('failures', transport.failure_publisher)
        self.assert_rkey_attr('outbound', transport.message_consumer)

    def mkmsg_ack(self, user_message_id='1', sent_message_id='abc',
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='ack',
            user_message_id=user_message_id,
            sent_message_id=sent_message_id,
            timestamp=UTCNearNow(),
            transport_name=self.transport_name,
            transport_metadata=transport_metadata,
            )

    def mkmsg_delivery(self, status='delivered', user_message_id='abc',
                       transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='delivery_report',
            transport_name=self.transport_name,
            user_message_id=user_message_id,
            delivery_status=status,
            to_addr='+41791234567',
            timestamp=UTCNearNow(),
            transport_metadata=transport_metadata,
            )

    def mkmsg_in(self, content='hello world', message_id='abc',
                 transport_type=None, transport_metadata=None):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr='+41791234567',
            to_addr='9292',
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            timestamp=UTCNearNow(),
            )

    def mkmsg_out(self, content='hello world', message_id='1',
                  to_addr='+41791234567', from_addr='9292',
                  in_reply_to=None, transport_type=None,
                  transport_metadata=None, stubs=False):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            in_reply_to=in_reply_to,
            )
        if stubs:
            params['timestamp'] = UTCNearNow()
        return TransportUserMessage(**params)

    def mkmsg_fail(self, message, reason,
                   failure_code=FailureMessage.FC_UNSPECIFIED):
        return FailureMessage(
            timestamp=UTCNearNow(),
            failure_code=failure_code,
            message=message,
            reason=reason,
            )

    def get_dispatched_events(self):
        return self._amqp.get_messages('vumi', self.rkey('event'))

    def get_dispatched_messages(self):
        return self._amqp.get_messages('vumi', self.rkey('inbound'))

    def get_dispatched_failures(self):
        return self._amqp.get_messages('vumi', self.rkey('failures'))

    def wait_for_dispatched_messages(self, amount):
        return self._amqp.wait_messages('vumi', self.rkey('inbound'), amount)

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('outbound')
        self._amqp.publish_message(exchange, rkey, message)
        return self._amqp.kick_delivery()


class BaseTransportTestCase(TransportTestCase):
    """
    This is a test for the base Transport class.

    Not to be confused with TransportTestCase above.
    """

    transport_name = 'carrier_pigeon'
    transport_class = Transport

    @inlineCallbacks
    def test_start_transport(self):
        tr = yield self.get_transport({})
        self.assertEqual(self.transport_name, tr.transport_name)
        self.assert_basic_rkeys(tr)

    @inlineCallbacks
    def test_consumer_registry(self):
        tr = yield self.get_transport({})
        self.assertEqual(len(tr._consumers), 1)
        yield tr.stopWorker()
        self.assertEqual(len(tr._consumers), 0)
