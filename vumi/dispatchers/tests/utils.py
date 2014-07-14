from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import VumiWorkerTestCase, PersistenceMixin

# For backcompat
from .helpers import DummyDispatcher
DummyDispatcher  # To keep pyflakes happy.


class DispatcherTestCase(VumiWorkerTestCase, PersistenceMixin):

    """
    This is a base class for testing dispatcher workers.

    """

    transport_name = None
    dispatcher_name = "sphex_dispatcher"
    dispatcher_class = None

    def setUp(self):
        self._persist_setUp()
        super(DispatcherTestCase, self).setUp()

    @inlineCallbacks
    def tearDown(self):
        yield super(DispatcherTestCase, self).tearDown()
        yield self._persist_tearDown()

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
        return self.get_worker(config, cls, start)

    def rkey(self, name):
        # We don't want the default behaviour for dispatchers.
        return name

    def get_dispatched_messages(self, transport_name, direction='outbound'):
        return self._get_dispatched(
            '%s.%s' % (transport_name, direction))

    def wait_for_dispatched_messages(self, transport_name, amount,
                                     direction='outbound'):
        return self._wait_for_dispatched(
            '%s.%s' % (transport_name, direction), amount)

    def dispatch(self, message, transport_name, direction='inbound',
                 exchange='vumi'):
        return self._dispatch(
            message, '%s.%s' % (transport_name, direction), exchange)
