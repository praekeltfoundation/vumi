from urllib import urlencode

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
            'web_path': '/api/v1/imimobile/ussd/some-suffix',
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

    def mk_request(self, **params):
        defaults = {
            'msisdn': '9221234567',
            'sms': 'Spam Spam Spam Spam Spammity Spam',
            'circle': 'Andhra Pradesh',
            'opnm': 'some-operator',
            'datetime': '1/26/2013 10:00:01 am'
        }
        defaults.update(params)

        return http_request_full('%s?%s' % (self.transport_url,
            urlencode(defaults)), data='', method='GET')

    @inlineCallbacks
    def test_inbound_begin(self):
        # Second connect is the actual start of the session
        user_content = "Who are you?"
        deferred = self.mk_request(sms=user_content)
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
                'datetime': '1/26/2013 10:00:01 am',
            },
        })

        reply_content = "We are the Knights Who Say ... Ni!"
        reply = TransportUserMessage(**msg.payload).reply(reply_content)
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response.delivered_body, reply_content)
        self.assertEqual(
            response.headers.getRawHeaders('X-USSD-SESSION'), ['1'])

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate session resume
        # note: imimobile do not provide a session id, so instead we use the
        # msisdn as the session id
        yield self.session_manager.create_session(
            '9221234567', to_addr='56263', from_addr='9221234567')

        user_content = "Well, what is it you want?"
        deferred = self.mk_request(sms=user_content)
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
                'datetime': '1/26/2013 10:00:01 am',
            },
        })

        reply_content = "We want ... a shrubbery!"
        reply = TransportUserMessage(**msg.payload).reply(
            reply_content, continue_session=False)
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response.delivered_body, reply_content)
        self.assertEqual(
            response.headers.getRawHeaders('X-USSD-SESSION'), ['0'])
