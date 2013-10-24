from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.cellulant import CellulantTransport
from vumi.message import TransportUserMessage
from vumi.utils import http_request
from vumi.tests.helpers import MessageHelper


class TestCellulantTransportTestCase(TransportTestCase):

    transport_class = CellulantTransport
    transport_name = 'test_cellulant'

    @inlineCallbacks
    def setUp(self):
        yield super(TestCellulantTransportTestCase, self).setUp()
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/ussd/cellulant/',
            'ussd_session_timeout': 60,
        }
        self.transport = yield self.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])
        yield self.transport.session_manager.redis._purge_all()  # just in case
        self.msg_helper = MessageHelper(transport_name=self.transport_name)

    def mk_request(self, **params):
        defaults = {
            'MSISDN': '27761234567',
            'INPUT': '',
            'opCode': 'BEG',
            'ABORT': '0',
            'sessionID': '1',
        }
        defaults.update(params)
        return http_request('%s?%s' % (self.transport_url,
            urlencode(defaults)), data='', method='GET')

    @inlineCallbacks
    def test_redis_caching(self):
        # delete the key that shouldn't exist (in case of testing real redis)
        yield self.transport.session_manager.redis.delete("msisdn:123")

        tx = self.transport
        val = yield tx.get_ussd_for_msisdn_session("msisdn", "123")
        self.assertEqual(None, val)
        yield tx.set_ussd_for_msisdn_session("msisdn", "123", "*bar#")
        val = yield tx.get_ussd_for_msisdn_session("msisdn", "123")
        self.assertEqual("*bar#", val)

    @inlineCallbacks
    def test_inbound_begin(self):
        deferred = self.mk_request(INPUT="*120*1#")

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], None)
        self.assertEqual(msg['to_addr'], '*120*1#')
        self.assertEqual(msg['from_addr'], '27761234567'),
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'session_id': '1',
        })

        reply = TransportUserMessage(**msg.payload).reply("ussd message")
        yield self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response, '1|ussd message|null|null|null|null')

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate prior BEG message
        yield self.transport.set_ussd_for_msisdn_session(
                '27761234567',
                '1',
                '*120*VERY_FAKE_CODE#',
                )
        deferred = self.mk_request(INPUT='hi', opCode='')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], 'hi')
        self.assertEqual(msg['to_addr'], '*120*VERY_FAKE_CODE#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['transport_metadata'], {
            'session_id': '1',
        })

        reply = TransportUserMessage(**msg.payload).reply("hello world",
            continue_session=False)
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response, '1|hello world|null|null|end|null')

    @inlineCallbacks
    def test_inbound_resume_with_failed_to_addr_lookup(self):
        deferred = self.mk_request(MSISDN='123456', INPUT='hi', opCode='')
        response = yield deferred
        self.assertEqual(response, '')

    @inlineCallbacks
    def test_inbound_abort_opcode(self):
        # first pre-populate the redis datastore to simulate prior BEG message
        yield self.transport.set_ussd_for_msisdn_session(
                '27761234567',
                '1',
                '*120*VERY_FAKE_CODE#',
                )
        # this one should return immediately with a blank
        # as there isn't going to be a sensible response
        resp = yield self.mk_request(opCode='ABO')
        self.assertEqual(resp, '')

        [msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_inbound_abort_field(self):
        # should also return immediately
        resp = yield self.mk_request(ABORT=1)
        self.assertEqual(resp, '')
        [msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_nack(self):
        msg = self.msg_helper.make_outbound("foo")
        self.dispatch(msg)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'], 'Missing fields: in_reply_to')
