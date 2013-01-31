import json
from urllib import urlencode
from datetime import datetime

from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi.message import TransportUserMessage
from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.imimobile import ImiMobileUssdTransport


class TestImiMobileUssdTransportTestCase(TransportTestCase):

    transport_class = ImiMobileUssdTransport
    timeout = 1

    @inlineCallbacks
    def setUp(self):
        yield super(TestImiMobileUssdTransportTestCase, self).setUp()
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/imimobile/ussd/',
            'suffix_to_addrs': {
                'some-suffix': '56263',
                'some-other-suffix': '56264',
             }
        }
        self.transport = yield self.get_transport(self.config)
        self.session_manager = self.transport.session_manager
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])
        yield self.session_manager.redis._purge_all()  # just in case

    def mk_full_request(self, suffix, **params):
        return http_request_full('%s?%s' % (self.transport_url + suffix,
            urlencode(params)), data='/api/v1/imimobile/ussd/', method='GET')

    def mk_request(self, suffix, **params):
        defaults = {
            'msisdn': '9221234567',
            'sms': 'Spam Spam Spam Spam Spammity Spam',
            'circle': 'Andhra Pradesh',
            'opnm': 'some-operator',
            'datetime': '1/26/2013 10:00:01 am'
        }
        defaults.update(params)
        return self.mk_full_request(suffix, **defaults)

    def mk_reply(self, request_msg, reply_content, continue_session=True):
        request_msg = TransportUserMessage(**request_msg.payload)
        return request_msg.reply(reply_content, continue_session)

    @inlineCallbacks
    def test_inbound_begin(self):
        # Second connect is the actual start of the session
        user_content = "Who are you?"
        d = self.mk_request('some-suffix', sms=user_content)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], user_content)
        self.assertEqual(msg['to_addr'], '56263')
        self.assertEqual(msg['from_addr'], '9221234567'),
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'imimobile_ussd': {
                'opnm': 'some-operator',
                'circle': 'Andhra Pradesh',
                'datetime': datetime(2013, 1, 26, 4, 30, 1),
            },
        })

        reply_content = "We are the Knights Who Say ... Ni!"
        reply = self.mk_reply(msg, reply_content)
        self.dispatch(reply)
        response = yield d
        self.assertEqual(response.delivered_body, reply_content)
        self.assertEqual(
            response.headers.getRawHeaders('X-USSD-SESSION'), ['1'])

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(ack.payload['event_type'], 'ack')
        self.assertEqual(ack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(ack.payload['sent_message_id'], reply['message_id'])

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate session resume
        # note: imimobile do not provide a session id, so instead we use the
        # msisdn as the session id
        yield self.session_manager.create_session(
            '9221234567', to_addr='56263', from_addr='9221234567')

        user_content = "Well, what is it you want?"
        d = self.mk_request('some-suffix', sms=user_content)
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], user_content)
        self.assertEqual(msg['to_addr'], '56263')
        self.assertEqual(msg['from_addr'], '9221234567')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['transport_metadata'], {
            'imimobile_ussd': {
                'opnm': 'some-operator',
                'circle': 'Andhra Pradesh',
                'datetime': datetime(2013, 1, 26, 4, 30, 1),
            },
        })

        reply_content = "We want ... a shrubbery!"
        reply = self.mk_reply(msg, reply_content, continue_session=False)
        self.dispatch(reply)
        response = yield d
        self.assertEqual(response.delivered_body, reply_content)
        self.assertEqual(
            response.headers.getRawHeaders('X-USSD-SESSION'), ['0'])

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(ack.payload['event_type'], 'ack')
        self.assertEqual(ack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(ack.payload['sent_message_id'], reply['message_id'])

    @inlineCallbacks
    def test_request_with_unknown_suffix(self):
        response = yield self.mk_request('unk-suffix')

        self.assertEqual(
            response.delivered_body,
            json.dumps({'unknown_suffix': 'unk-suffix'}))
        self.assertEqual(response.code, 400)

    @inlineCallbacks
    def test_request_with_missing_parameters(self):
        response = yield self.mk_full_request(
            'some-suffix', sms='', circle='', opnm='')

        self.assertEqual(
            response.delivered_body,
            json.dumps({'missing_parameter': ['msisdn', 'datetime']}))
        self.assertEqual(response.code, 400)

    @inlineCallbacks
    def test_request_with_unexpected_parameters(self):
        response = yield self.mk_request(
            'some-suffix', unexpected_p1='', unexpected_p2='')

        self.assertEqual(
            response.delivered_body,
            json.dumps({
                'unexpected_parameter': ['unexpected_p1', 'unexpected_p2']
            }))
        self.assertEqual(response.code, 400)

    @inlineCallbacks
    def test_nack_insufficient_message_fields(self):
        msg = self.mkmsg_out(message_id='23', in_reply_to=None, content=None)
        self.dispatch(msg)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(nack.payload['event_type'], 'nack')
        self.assertEqual(nack.payload['user_message_id'], '23')
        self.assertEqual(nack.payload['nack_reason'],
                         self.transport.ERRORS['INSUFFICIENT_MSG_FIELDS'])

    @inlineCallbacks
    def test_nack_http_http_response_failure(self):
        self.patch(self.transport, 'finish_request', lambda *a, **kw: None)
        msg = self.mkmsg_out(
            message_id='23',
            in_reply_to='some-number',
            content='There are some who call me ... Tim!')
        self.dispatch(msg)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(nack.payload['event_type'], 'nack')
        self.assertEqual(nack.payload['user_message_id'], '23')
        self.assertEqual(nack.payload['nack_reason'],
                         self.transport.ERRORS['RESPONSE_FAILURE'])

    def test_ist_to_utc(self):
        self.assertEqual(
            ImiMobileUssdTransport.ist_to_utc("1/26/2013 03:30:00 pm"),
            datetime(2013, 1, 26, 10, 0, 0))

        self.assertEqual(
            ImiMobileUssdTransport.ist_to_utc("01/29/2013 04:53:59 am"),
            datetime(2013, 1, 28, 23, 23, 59))

        self.assertEqual(
            ImiMobileUssdTransport.ist_to_utc("01/31/2013 07:20:00 pm"),
            datetime(2013, 1, 31, 13, 50, 0))

        self.assertEqual(
            ImiMobileUssdTransport.ist_to_utc("3/8/2013 8:5:5 am"),
            datetime(2013, 3, 8, 2, 35, 5))
