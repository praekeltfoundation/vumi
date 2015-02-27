import logging

from twisted.internet.defer import inlineCallbacks

from vumi.application.sandbox.resources.logging import LoggingResource
from vumi.application.sandbox.resources.tests.utils import (
    ResourceTestCaseBase)
from vumi.tests.utils import LogCatcher


class TestLoggingResource(ResourceTestCaseBase):

    resource_cls = LoggingResource

    @inlineCallbacks
    def setUp(self):
        super(TestLoggingResource, self).setUp()
        yield self.create_resource({})

    @inlineCallbacks
    def check_logs(self, cmd_name, msg, log_level, **kw):
        with LogCatcher(log_level=log_level) as lc:
            reply = yield self.dispatch_command(cmd_name, msg=msg, **kw)
            msgs = lc.messages()
        self.assertEqual(reply['success'], True)
        self.assertEqual(msgs, [msg])

    def test_handle_debug(self):
        return self.check_logs('debug', 'foo', logging.DEBUG)

    def test_handle_info(self):
        return self.check_logs('info', 'foo', logging.INFO)

    def test_handle_warning(self):
        return self.check_logs('warning', 'foo', logging.WARNING)

    def test_handle_error(self):
        return self.check_logs('error', 'foo', logging.ERROR)

    def test_handle_critical(self):
        return self.check_logs('critical', 'foo', logging.CRITICAL)

    def test_handle_log(self):
        return self.check_logs('log', 'foo', logging.ERROR,
                               level=logging.ERROR)

    def test_handle_log_defaults_to_info(self):
        return self.check_logs('log', 'foo', logging.INFO)

    @inlineCallbacks
    def test_with_unicode(self):
        with LogCatcher() as lc:
            reply = yield self.dispatch_command('log', msg=u'Zo\u00eb')
            msgs = lc.messages()
        self.assertEqual(reply['success'], True)
        self.assertEqual(msgs, ['Zo\xc3\xab'])
