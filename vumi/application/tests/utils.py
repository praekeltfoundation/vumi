from datetime import datetime

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.tests.utils import get_stubbed_worker, PersistenceMixin


class ApplicationTestCase(TestCase, PersistenceMixin):

    """
    This is a base class for testing application workers.

    """

    # base timeout of 5s for all application tests
    timeout = 5

    transport_name = "sphex"
    transport_type = None
    application_class = None

    def setUp(self):
        self._workers = []
        self._amqp = FakeAMQPBroker()
        self._persist_setUp()

    @inlineCallbacks
    def tearDown(self):
        for worker in self._workers:
            yield worker.stopWorker()
        yield self._persist_tearDown()

    def rkey(self, name):
        return "%s.%s" % (self.transport_name, name)

    @inlineCallbacks
    def get_application(self, config, cls=None, start=True):
        """
        Get an instance of a worker class.

        :param config: Config dict.
        :param cls: The Application class to instantiate.
                    Defaults to :attr:`application_class`
        :param start: True to start the application (default), False otherwise.

        Some default config values are helpfully provided in the
        interests of reducing boilerplate:

        * ``transport_name`` defaults to :attr:`self.transport_name`
        * ``send_to`` defaults to a dictionary with config for each tag
          defined in worker's SEND_TO_TAGS attribute. Each tag's config
          contains a transport_name set to ``<tag>_outbound``.
        """

        if cls is None:
            cls = self.application_class
        config = self.mk_config(config)
        config.setdefault('transport_name', self.transport_name)
        if 'send_to' not in config and cls.SEND_TO_TAGS:
            config['send_to'] = {}
            for tag in cls.SEND_TO_TAGS:
                config['send_to'][tag] = {
                    'transport_name': '%s_outbound' % tag}
        worker = get_stubbed_worker(cls, config, self._amqp)
        self._workers.append(worker)
        if start:
            yield worker.startWorker()
        returnValue(worker)

    def mkmsg_in(self, content='hello world', message_id='abc',
                 to_addr='9292', from_addr='+41791234567', group=None,
                 session_event=None, transport_type=None,
                 helper_metadata=None, transport_metadata=None):
        if transport_type is None:
            transport_type = self.transport_type
        if helper_metadata is None:
            helper_metadata = {}
        if transport_metadata is None:
            transport_metadata = {}
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr=to_addr,
            group=group,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            helper_metadata=helper_metadata,
            content=content,
            session_event=session_event,
            timestamp=datetime.now(),
            )

    def mkmsg_out(self, content='hello world', message_id='1',
                  to_addr='+41791234567', from_addr='9292', group=None,
                  session_event=None, in_reply_to=None,
                  transport_type=None, transport_metadata=None,
                  ):
        if transport_type is None:
            transport_type = self.transport_type
        if transport_metadata is None:
            transport_metadata = {}
        params = dict(
            to_addr=to_addr,
            from_addr=from_addr,
            group=group,
            message_id=message_id,
            transport_name=self.transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            content=content,
            session_event=session_event,
            in_reply_to=in_reply_to,
            )
        return TransportUserMessage(**params)

    def mkmsg_ack(self, user_message_id='1', sent_message_id='abc',
                  transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_type='ack',
            user_message_id=user_message_id,
            sent_message_id=sent_message_id,
            transport_name=self.transport_name,
            transport_metadata=transport_metadata,
            )

    def mkmsg_delivery(self, status='delivered', user_message_id='abc',
                       transport_metadata=None):
        if transport_metadata is None:
            transport_metadata = {}
        return TransportEvent(
            event_type='delivery_report',
            transport_name=self.transport_name,
            user_message_id=user_message_id,
            delivery_status=status,
            to_addr='+41791234567',
            transport_metadata=transport_metadata,
            )

    def get_dispatched_messages(self):
        return self._amqp.get_messages('vumi', self.rkey('outbound'))

    def wait_for_dispatched_messages(self, amount):
        return self._amqp.wait_messages('vumi', self.rkey('outbound'), amount)

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('inbound')
        self._amqp.publish_message(exchange, rkey, message)
        return self._amqp.kick_delivery()
