# -*- test-case-name: vumi.application.tests.test_sandbox -*-

"""An application for sandboxing message processing."""

from vumi.application.base import ApplicationWorker


class Sandbox(ApplicationWorker):

    def validate_config(self):
        pass

    def setup_application(self):
        pass

    def teardown_application(self):
        pass

    def consume_user_message(self, event):
        pass

    def close_session(self, event):
        pass

    def consume_ack(self, event):
        pass

    def consume_delivery_report(self, event):
        pass
