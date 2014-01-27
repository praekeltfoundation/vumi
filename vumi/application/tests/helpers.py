# -*- test-case-name: vumi.application.tests.test_test_helpers -*-

from twisted.internet.defer import inlineCallbacks

from zope.interface import implements

from vumi.tests.helpers import (
    MessageHelper, WorkerHelper, MessageDispatchHelper, PersistenceHelper,
    generate_proxies, IHelper,
)


class ApplicationHelper(object):
    """
    Test helper for application workers.

    This helper construct and wraps several lower-level helpers and provides
    higher-level functionality for app worker tests.

    :param application_class:
        The worker class for the application being tested.

    :param bool use_riak:
        Set to ``True`` if the test requires Riak. This is passed to the
        underlying :class:`~vumi.tests.helpers.PersistenceHelper`.

    :param \**kw:
        All other keyword params are passed to the underlying
        :class:`~vumi.tests.helpers.PersistenceHelper`.
    """

    implements(IHelper)

    def __init__(self, application_class, use_riak=False, **msg_helper_args):
        self.application_class = application_class
        self.persistence_helper = PersistenceHelper(use_riak=use_riak)
        self.msg_helper = MessageHelper(**msg_helper_args)
        self.transport_name = self.msg_helper.transport_name
        self.worker_helper = WorkerHelper(self.msg_helper.transport_name)
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
        config.setdefault('transport_name', self.msg_helper.transport_name)
        return self.get_worker(cls, config, start)
