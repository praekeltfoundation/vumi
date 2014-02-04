from twisted.internet.defer import inlineCallbacks

from zope.interface import implements

from vumi.dispatchers.base import BaseDispatchWorker
from vumi.middleware import MiddlewareStack
from vumi.tests.helpers import (
    MessageHelper, PersistenceHelper, WorkerHelper, MessageDispatchHelper,
    generate_proxies, IHelper,
)


class DummyDispatcher(BaseDispatchWorker):

    class DummyPublisher(object):
        def __init__(self):
            self.msgs = []

        def publish_message(self, msg):
            self.msgs.append(msg)

        def clear(self):
            self.msgs[:] = []

    def __init__(self, config):
        self.transport_publisher = {}
        self.transport_names = config.get('transport_names', [])
        for transport in self.transport_names:
            self.transport_publisher[transport] = self.DummyPublisher()
        self.exposed_publisher = {}
        self.exposed_event_publisher = {}
        self.exposed_names = config.get('exposed_names', [])
        for exposed in self.exposed_names:
            self.exposed_publisher[exposed] = self.DummyPublisher()
            self.exposed_event_publisher[exposed] = self.DummyPublisher()
        self._middlewares = MiddlewareStack([])


class DispatcherHelper(object):
    """
    Test helper for dispatcher workers.

    This helper construct and wraps several lower-level helpers and provides
    higher-level functionality for dispatcher tests.

    :param dispatcher_class:
        The worker class for the dispatcher being tested.

    :param bool use_riak:
        Set to ``True`` if the test requires Riak. This is passed to the
        underlying :class:`~vumi.tests.helpers.PersistenceHelper`.

    :param \**msg_helper_args:
        All other keyword params are passed to the underlying
        :class:`~vumi.tests.helpers.MessageHelper`.
    """

    implements(IHelper)

    def __init__(self, dispatcher_class, use_riak=False, **msg_helper_args):
        self.dispatcher_class = dispatcher_class
        self.worker_helper = WorkerHelper()
        self.persistence_helper = PersistenceHelper(use_riak=use_riak)
        self.msg_helper = MessageHelper(**msg_helper_args)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.worker_helper)

        # Proxy methods from our helpers.
        generate_proxies(self, self.msg_helper)
        generate_proxies(self, self.worker_helper)
        generate_proxies(self, self.dispatch_helper)

    def setup(self):
        self.persistence_helper.setup()
        self.worker_helper.setup()

    @inlineCallbacks
    def cleanup(self):
        yield self.worker_helper.cleanup()
        yield self.persistence_helper.cleanup()

    def get_dispatcher(self, config, cls=None, start=True):
        """
        Get an instance of a dispatcher class.

        :param dict config: Config dict.
        :param cls:
            The transport class to instantiate. Defaults to
            :attr:`dispatcher_class`
        :param bool start:
            ``True`` to start the dispatcher (default), ``False`` otherwise.
        """
        if cls is None:
            cls = self.dispatcher_class
        config = self.persistence_helper.mk_config(config)
        return self.get_worker(cls, config, start)

    def get_connector_helper(self, connector_name):
        """
        Construct a :class:`~DispatcherConnectorHelper` for the provided
        ``connector_name``.
        """
        return DispatcherConnectorHelper(self, connector_name)


class DispatcherConnectorHelper(object):
    """
    Subset of :class:`~vumi.tests.helpers.WorkerHelper` and
    :class:`~vumi.tests.helpers.MessageDispatchHelper` functionality for a
    specific connector. This should only be created with
    :meth:`DispatcherHelper.get_connector_helper`.
    """
    def __init__(self, dispatcher_helper, connector_name):
        self.msg_helper = dispatcher_helper.msg_helper
        self.worker_helper = WorkerHelper(
            connector_name, dispatcher_helper.worker_helper.broker)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.worker_helper)

        generate_proxies(self, self.worker_helper)
        generate_proxies(self, self.dispatch_helper)

        # We don't want to be able to make workers with this helper.
        del self.get_worker
        del self.cleanup_worker
