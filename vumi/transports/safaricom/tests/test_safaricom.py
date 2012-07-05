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
    timeout = 1

    @inlineCallbacks
    def setUp(self):
        yield super(TestSafaricomTransportTestCase, self).setUp()
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/safaricom/ussd/',
            'redis': {},
        }
        self.patch(SafaricomTransport, 'connect_to_redis',
            lambda s: FakeRedis(**s.redis_config))
        self.transport = yield self.get_transport(self.config)
        self.session_manager = self.transport.session_manager
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
            'DEST': '167',
            'SESSION_ID': 'session-id',
            'USSD_PARAMS': '',
        }
        defaults.update(params)
        return self.mk_full_request(**defaults)

    @inlineCallbacks
    def test_inbound_begin(self):
        # If we get a menu like *167*7# the USSD_PARAMS is '7'
        # if we get a * in it, it is a follow up request, so '7*a' means
        # the user submitted 'a'
        deferred = self.mk_request(USSD_PARAMS='7')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '')
        self.assertEqual(msg['to_addr'], '*167*7#')
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
        self.session_manager.create_session('session-id',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='7*a*b')
        # Safaricom gives us the history of the full session in the USSD_PARAMS
        # The last submitted bit of content is the last value delimited by '*'
        deferred = self.mk_request(USSD_PARAMS='7*a*b*c')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], 'c')
        self.assertEqual(msg['to_addr'], '*167*7#')
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
            USSD_PARAMS='7*a', SESSION_ID='session-id')
        response = yield deferred
        self.assertEqual(json.loads(response), {
            'missing_parameter': ['DEST'],
        })

    @inlineCallbacks
    def test_to_addr_handling(self):
        defaults = {
            'DEST': '167',
            'ORIG': '12345',
            'SESSION_ID': 'session-id',
        }
        # initial connect
        d1 = self.mk_full_request(USSD_PARAMS='7*1', **defaults)
        [msg1] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg1['to_addr'], '*167*7*1#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        reply = TransportUserMessage(**msg1.payload).reply("hello world",
            continue_session=True)
        self.dispatch(reply)
        yield d1

        # follow up with the user submitting 'a'
        d2 = self.mk_full_request(USSD_PARAMS='7*1*a', **defaults)
        [msg1, msg2] = yield self.wait_for_dispatched_messages(2)
        self.assertEqual(msg2['to_addr'], '*167*7*1#')
        self.assertEqual(msg2['content'], 'a')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        reply = TransportUserMessage(**msg2.payload).reply("hello world",
            continue_session=False)
        self.dispatch(reply)
        yield d2

    @inlineCallbacks
    def test_hitting_url_twice_without_content(self):
        d1 = self.mk_request(USSD_PARAMS='7*3')
        [msg1] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg1['to_addr'], '*167*7*3#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        reply = TransportUserMessage(**msg1.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield d1

        # make the exact same request again
        d2 = self.mk_request(USSD_PARAMS='7*3')
        [msg1, msg2] = yield self.wait_for_dispatched_messages(2)
        self.assertEqual(msg2['to_addr'], '*167*7*3#')
        self.assertEqual(msg2['content'], '')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        reply = TransportUserMessage(**msg2.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield d2

    @inlineCallbacks
    def test_submitting_asterisks_as_values(self):
        self.session_manager.create_session('session-id',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='7*a*b')
        # we're submitting a bunch of *s
        deferred = self.mk_request(USSD_PARAMS='7*a*b*****')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '****')

        reply = TransportUserMessage(**msg.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield deferred
        session = self.session_manager.load_session('session-id')
        self.assertEqual(session['last_ussd_params'], '7*a*b*****')

    @inlineCallbacks
    def test_submitting_asterisks_as_values_after_asterisks(self):
        self.session_manager.create_session('session-id',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='7*a*b**')
        # we're submitting a bunch of *s
        deferred = self.mk_request(USSD_PARAMS='7*a*b*****')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '**')

        reply = TransportUserMessage(**msg.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield deferred
        session = self.session_manager.load_session('session-id')
        self.assertEqual(session['last_ussd_params'], '7*a*b*****')
