from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import VumiWorkerTestCase, PersistenceMixin


class TransportTestCase(VumiWorkerTestCase, PersistenceMixin):
    """
    This is a base class for testing transports.
    """

    transport_class = None

    def setUp(self):
        self._persist_setUp()
        super(TransportTestCase, self).setUp()

    @inlineCallbacks
    def tearDown(self):
        yield super(TransportTestCase, self).tearDown()
        yield self._persist_tearDown()

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
        config = self.mk_config(config)
        config.setdefault('transport_name', self.transport_name)
        return self.get_worker(config, cls, start)

    def mkmsg_in(self, *args, **kw):
        msg = super(TransportTestCase, self).mkmsg_in(*args, **kw)
        return self._make_matcher(msg)

    def mkmsg_ack(self, *args, **kw):
        msg = super(TransportTestCase, self).mkmsg_ack(*args, **kw)
        return self._make_matcher(msg, 'event_id')

    def mkmsg_delivery(self, *args, **kw):
        msg = super(TransportTestCase, self).mkmsg_delivery(*args, **kw)
        return self._make_matcher(msg, 'event_id')

    def get_dispatched_messages(self):
        return self.get_dispatched_inbound()

    def wait_for_dispatched_messages(self, amount):
        return self.wait_for_dispatched_inbound(amount)

    def clear_dispatched_messages(self):
        return self.clear_dispatched_inbound()

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('outbound')
        return self._dispatch(message, rkey, exchange)
