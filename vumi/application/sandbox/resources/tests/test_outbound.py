from twisted.internet.defer import succeed, inlineCallbacks

from vumi.application.sandbox.resources.outbound import OutboundResource
from vumi.application.sandbox.resources.tests.utils import (
    ResourceTestCaseBase)


class TestOutboundResource(ResourceTestCaseBase):

    resource_cls = OutboundResource

    @inlineCallbacks
    def setUp(self):
        super(TestOutboundResource, self).setUp()
        yield self.create_resource({})

    @inlineCallbacks
    def test_handle_reply_to(self):
        self.app_worker.mock_returns['reply_to'] = succeed(None)
        self.api.get_inbound_message = lambda msg_id: msg_id
        reply = yield self.dispatch_command('reply_to', content='hello',
                                            continue_session=True,
                                            in_reply_to='msg1')
        self.check_reply(reply, success=True)
        self.assertEqual(self.app_worker.mock_calls['reply_to'],
                         [(('msg1', 'hello'), {'continue_session': True})])

    @inlineCallbacks
    def test_handle_reply_to_group(self):
        self.app_worker.mock_returns['reply_to_group'] = succeed(None)
        self.api.get_inbound_message = lambda msg_id: msg_id
        reply = yield self.dispatch_command('reply_to_group', content='hello',
                                            continue_session=True,
                                            in_reply_to='msg1')
        self.check_reply(reply, success=True)
        self.assertEqual(self.app_worker.mock_calls['reply_to_group'],
                         [(('msg1', 'hello'), {'continue_session': True})])

    @inlineCallbacks
    def test_handle_send_to(self):
        self.app_worker.mock_returns['send_to'] = succeed(None)
        reply = yield self.dispatch_command('send_to', content='hello',
                                            to_addr='1234',
                                            tag='default')
        self.check_reply(reply, success=True)
        self.assertEqual(self.app_worker.mock_calls['send_to'],
                         [(('1234', 'hello'), {'endpoint': 'default'})])
