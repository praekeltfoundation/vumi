from vumi.tests.helpers import (
    MessageHelper, AMQPHelper, MessageDispatchHelper, generate_proxies,
)


class DispatcherHelper(object):
    # TODO: Decide if we actually want to pass the test case in here.
    #       We currently do this because we need to get at .mk_config from the
    #       persistence mixin. This should be going away soon when the
    #       persistence stuff becomes a helper.
    def __init__(self, dispatcher_class, test_case, msg_helper_args=None):
        self._test_case = test_case
        self._dispatcher_class = dispatcher_class
        self.amqp_helper = AMQPHelper()
        msg_helper_kw = {}
        if msg_helper_args is not None:
            msg_helper_kw.update(msg_helper_args)
        self.msg_helper = MessageHelper(**msg_helper_kw)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.amqp_helper)

        # Proxy methods from our helpers.
        generate_proxies(self, self.msg_helper)
        generate_proxies(self, self.amqp_helper)
        generate_proxies(self, self.dispatch_helper)

    def cleanup(self):
        return self.amqp_helper.cleanup()

    def get_dispatcher(self, config, cls=None, start=True):
        if cls is None:
            cls = self._dispatcher_class
        # We might need to do persistence config mangling.
        if hasattr(self._test_case, 'mk_config'):
            config = self._test_case.mk_config(config)
        return self.get_worker(cls, config, start)

    def get_connector_helper(self, connector_name):
        return DispatcherConnectorHelper(self, connector_name)


class DispatcherConnectorHelper(object):
    def __init__(self, dispatcher_helper, connector_name):
        self.msg_helper = dispatcher_helper.msg_helper
        self.amqp_helper = AMQPHelper(
            connector_name, dispatcher_helper.amqp_helper.broker)
        self.dispatch_helper = MessageDispatchHelper(
            self.msg_helper, self.amqp_helper)

        generate_proxies(self, self.amqp_helper)
        generate_proxies(self, self.dispatch_helper)

        # We don't want to be able to make workers with this helper.
        del self.get_worker
        del self.cleanup_worker
