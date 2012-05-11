import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.safaricom import SafaricomTransport
from vumi.message import TransportUserMessage
from vumi.utils import http_request
from vumi.tests.utils import FakeRedis


class TestSafaricomTransportTestCase(TransportTestCase):

    transport_class = SafaricomTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestSafaricomTransportTestCase, self).setUp()
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/safaricom/ussd/',
            'redis': {},
        }
        self.transport = yield self.get_transport(self.config)
        self.transport.r_server = FakeRedis()
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])

    @inlineCallbacks
    def tearDown(self):
        yield super(TestSafaricomTransportTestCase, self).tearDown()
        self.transport.r_server.teardown()

    def mk_full_request(self, **params):
        return http_request('%s?%s' % (self.transport_url,
            urlencode(params)), data='', method='GET')

    def mk_request(self, **params):
        defaults = {
            'ORIG': '27761234567',
            'DEST': '*120*123#',
            'SESSION_ID': 'session-id',
            'USSD_PARAMS': '',
        }
        defaults.update(params)
        return self.mk_full_request(**defaults)

    @inlineCallbacks
    def test_inbound_begin(self):
        deferred = self.mk_request()

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '')
        self.assertEqual(msg['to_addr'], '*120*123#')
        self.assertEqual(msg['from_addr'], '27761234567'),
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'safaricom': {
                'session_id': 'session-id',
            },
        })

        reply = TransportUserMessage(**msg.payload).reply("ussd message")
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response, 'CON ussd message')

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate prior BEG message
        self.transport.set_ussd_for_msisdn_session(
                '27761234567',  # msisdn
                'session-id',   # the session id
                '*120*123#',    # the DEST parameter
                )
        deferred = self.mk_request(USSD_PARAMS='hi')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], 'hi')
        self.assertEqual(msg['to_addr'], '*120*123#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['transport_metadata'], {
            'safaricom': {
                'session_id': 'session-id',
            },
        })

        reply = TransportUserMessage(**msg.payload).reply("hello world",
            continue_session=False)
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response, 'END hello world')

    @inlineCallbacks
    def test_inbound_resume_with_failed_to_addr_lookup(self):
        deferred = self.mk_full_request(ORIG='123456',
            USSD_PARAMS='hi', SESSION_ID='session-id')
        response = yield deferred
        self.assertEqual(json.loads(response), {
            'missing_parameter': ['DEST'],
        })

    @inlineCallbacks
    def test_inbound_abort_field(self):
        # should also return immediately
        resp = yield self.mk_request(ABORT=1)
        self.assertEqual(resp, '')
        [msg] = yield self.get_dispatched_messages()
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)
