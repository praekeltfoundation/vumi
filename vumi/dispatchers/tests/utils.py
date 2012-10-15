from datetime import datetime

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.utils import get_stubbed_worker, PersistenceMixin
from vumi.tests.fake_amqp import FakeAMQPBroker


class DispatcherTestCase(TestCase, PersistenceMixin):

    """
    This is a base class for testing dispatcher workers.

    """

    # base timeout of 5s for all dispatcher tests
    timeout = 5

    dispatcher_name = "sphex_dispatcher"
    dispatcher_class = None

    def setUp(self):
        self._workers = []
        self._amqp = FakeAMQPBroker()
        self._persist_setUp()

    @inlineCallbacks
    def tearDown(self):
        for worker in self._workers:
            yield worker.stopWorker()
        yield self._persist_tearDown()

    @inlineCallbacks
    def get_dispatcher(self, config, cls=None, start=True):
        """
        Get an instance of a dispatcher class.

        :param config: Config dict.
        :param cls: The Dispatcher class to instantiate.
                    Defaults to :attr:`dispatcher_class`
        :param start: True to start the displatcher (default), False otherwise.

        Some default config values are helpfully provided in the
        interests of reducing boilerplate:

        * ``dispatcher_name`` defaults to :attr:`self.dispatcher_name`
        """

        if cls is None:
            cls = self.dispatcher_class
        config = self.mk_config(config)
        config.setdefault('dispatcher_name', self.dispatcher_name)
        worker = get_stubbed_worker(cls, config, self._amqp)
        self._workers.append(worker)
        if start:
            yield worker.startWorker()
        returnValue(worker)

    def mkmsg_in(self, content='hello world', message_id='abc',
                 to_addr='9292', from_addr='+41791234567',
                 session_event=None, transport_type=None,
                 helper_metadata=None, transport_metadata=None,
                 transport_name=None):
        if helper_metadata is None:
            helper_metadata = {}
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr=to_addr,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            helper_metadata=helper_metadata,
            content=content,
            session_event=session_event,
            timestamp=datetime.now(),
            )

    def mkmsg_out(self, content='hello world', message_id='1',
                  to_addr='+41791234567', from_addr='9292',
                  session_event=None, in_reply_to=None,
                  transport_type=None, transport_metadata=None,
                  transport_name=None):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            )
        return TransportUserMessage(**params)

    def mkmsg_ack(self, event_type='ack', user_message_id='1',
                  send_message_id='abc', transport_name=None,
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            event_type=event_type,
            user_message_id=user_message_id,
            sent_message_id=send_message_id,
            transport_name=transport_name,
            transport_metadata=transport_metadata,
            )
        return TransportEvent(**params)

    def get_dispatched_messages(self, transport_name, direction='outbound'):
        return self._amqp.get_messages('vumi', '%s.%s' % (
            transport_name, direction))

    def wait_for_dispatched_messages(self, transport_name, amount,
                                        direction='outbound'):
        return self._amqp.wait_messages('vumi', '%s.%s' % (
            transport_name, direction), amount)

    def dispatch(self, message, transport_name, direction='inbound',
                    exchange='vumi'):
        rkey = '%s.%s' % (transport_name, direction)
        self._amqp.publish_message(exchange, rkey, message)
        return self._amqp.kick_delivery()
