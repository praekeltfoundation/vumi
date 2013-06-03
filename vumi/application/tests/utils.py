from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import VumiWorkerTestCase, PersistenceMixin


class ApplicationTestCase(VumiWorkerTestCase, PersistenceMixin):

    """
    This is a base class for testing application workers.

    """

    application_class = None

    def setUp(self):
        self._persist_setUp()
        super(ApplicationTestCase, self).setUp()

    @inlineCallbacks
    def tearDown(self):
        yield super(ApplicationTestCase, self).tearDown()
        yield self._persist_tearDown()

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
        """

        if cls is None:
            cls = self.application_class
        config = self.mk_config(config)
        config.setdefault('transport_name', self.transport_name)
        return self.get_worker(config, cls, start)

    def get_dispatched_messages(self):
        return self.get_dispatched_outbound()

    def wait_for_dispatched_messages(self, amount):
        return self.wait_for_dispatched_outbound(amount)

    def clear_dispatched_messages(self):
        return self.clear_dispatched_outbound()

    def dispatch(self, message, rkey=None, exchange='vumi'):
        if rkey is None:
            rkey = self.rkey('inbound')
        return self._dispatch(message, rkey, exchange)
