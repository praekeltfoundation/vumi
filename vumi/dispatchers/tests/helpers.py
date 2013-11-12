from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.base import BaseDispatchWorker
from vumi.middleware import MiddlewareStack
from vumi.tests.helpers import (
    MessageHelper, PersistenceHelper, WorkerHelper, MessageDispatchHelper,
    generate_proxies,
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
    # TODO: Decide if we actually want to pass the test case in here.
    #       We currently do this because we need to get at .mk_config from the
    #       persistence mixin. This should be going away soon when the
    #       persistence stuff becomes a helper.
    def __init__(self, dispatcher_class, test_case, msg_helper_args=None):
        self._test_case = test_case
        self._dispatcher_class = dispatcher_class
        self.worker_helper = WorkerHelper()
        self.persistence_helper = PersistenceHelper()
        msg_helper_kw = {}
        if msg_helper_args is not None:
            msg_helper_kw.update(msg_helper_args)
        self.msg_helper = MessageHelper(**msg_helper_kw)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.worker_helper)

        # Proxy methods from our helpers.
        generate_proxies(self, self.msg_helper)
        generate_proxies(self, self.worker_helper)
        generate_proxies(self, self.dispatch_helper)

    @inlineCallbacks
    def cleanup(self):
        yield self.worker_helper.cleanup()
        yield self.persistence_helper.cleanup()

    def get_dispatcher(self, config, cls=None, start=True):
        if cls is None:
            cls = self._dispatcher_class
        config = self.persistence_helper.mk_config(config)
        return self.get_worker(cls, config, start)

    def get_connector_helper(self, connector_name):
        return DispatcherConnectorHelper(self, connector_name)


class DispatcherConnectorHelper(object):
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
