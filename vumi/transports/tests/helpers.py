from twisted.internet.defer import inlineCallbacks

from zope.interface import implements

from vumi.transports.failures import FailureMessage
from vumi.tests.helpers import (
    MessageHelper, WorkerHelper, PersistenceHelper, MessageDispatchHelper,
    generate_proxies, IHelper,
)


class TransportHelper(object):
    """
    Test helper for transport workers.

    This helper construct and wraps several lower-level helpers and provides
    higher-level functionality for transport tests.

    :param transport_class:
        The worker class for the transport being tested.

    :param bool use_riak:
        Set to ``True`` if the test requires Riak. This is passed to the
        underlying :class:`~vumi.tests.helpers.PersistenceHelper`.

    :param \**msg_helper_args:
        All other keyword params are passed to the underlying
        :class:`~vumi.tests.helpers.MessageHelper`.
    """

    implements(IHelper)

    def __init__(self, transport_class, use_riak=False, **msg_helper_args):
        self.transport_class = transport_class
        self.persistence_helper = PersistenceHelper(use_riak=use_riak)
        self.msg_helper = MessageHelper(**msg_helper_args)
        self.transport_name = self.msg_helper.transport_name
        self.worker_helper = WorkerHelper(self.transport_name)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.worker_helper)

        # Proxy methods from our helpers.
        generate_proxies(self, self.msg_helper)
        generate_proxies(self, self.worker_helper)
        generate_proxies(self, self.dispatch_helper)
        generate_proxies(self, self.persistence_helper)

    def setup(self):
        self.persistence_helper.setup()
        self.worker_helper.setup()

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
            cls = self.transport_class
        config = self.mk_config(config)
        config.setdefault('transport_name', self.transport_name)
        return self.get_worker(cls, config, start)

    def get_dispatched_failures(self, connector_name=None):
        """
        Get failures dispatched by a transport.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A list of :class:`~vumi.transports.failures.FailureMessage`
            instances.
        """
        return self.get_dispatched(connector_name, 'failures', FailureMessage)
