from twisted.internet.defer import inlineCallbacks

from vumi.transports.failures import FailureMessage
from vumi.tests.helpers import (
    MessageHelper, WorkerHelper, PersistenceHelper, MessageDispatchHelper,
    generate_proxies,
)


class TransportHelper(object):
    # TODO: Decide if we actually want to pass the test case in here.
    #       We currently do this for two reasons:
    #       1. We need to get at .mk_config from the persistence mixin. This
    #          should be going away soon when the persistence stuff becomes a
    #          helper.
    #       2. We look at all the test setup class attributes (.transport_name,
    #          .transport_class, etc.) to avoid passing them into various
    #          methods. This can probably be avoided with a little effort.
    def __init__(self, test_case, transport_name=None, msg_helper_args=None):
        self._test_case = test_case
        self.persistence_helper = PersistenceHelper()
        msg_helper_kw = {}
        if msg_helper_args is not None:
            msg_helper_kw.update(msg_helper_args)
        if transport_name is not None:
            msg_helper_kw['transport_name'] = transport_name
        self.msg_helper = MessageHelper(**msg_helper_kw)
        self.transport_name = self.msg_helper.transport_name
        self.worker_helper = WorkerHelper(self.transport_name)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.worker_helper)

        # Proxy methods from our helpers.
        generate_proxies(self, self.msg_helper)
        generate_proxies(self, self.worker_helper)
        generate_proxies(self, self.dispatch_helper)
        generate_proxies(self, self.persistence_helper)

    @inlineCallbacks
    def cleanup(self):
        yield self.worker_helper.cleanup()
        yield self.persistence_helper.cleanup()

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
            cls = self._test_case.transport_class
        config = self.mk_config(config)
        config.setdefault('transport_name', self.transport_name)
        return self.get_worker(cls, config, start)

    def get_dispatched_failures(self, connector_name=None):
        return self.get_dispatched(connector_name, 'failures', FailureMessage)
