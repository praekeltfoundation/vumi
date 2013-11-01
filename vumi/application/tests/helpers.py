from vumi.tests.helpers import (
    MessageHelper, AMQPHelper, MessageDispatchHelper, generate_proxies,
)


class ApplicationHelper(object):
    # TODO: Decide if we actually want to pass the test case in here.
    def __init__(self, test_case, msg_helper_args=None):
        self._test_case = test_case
        self.amqp_helper = AMQPHelper(self._test_case.transport_name)
        msg_helper_kw = {
            'transport_name': self._test_case.transport_name,
        }
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
            cls = self._test_case.application_class
        config = self._test_case.mk_config(config)
        config.setdefault('transport_name', self._test_case.transport_name)
        return self.get_worker(cls, config, start)
