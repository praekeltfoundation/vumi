import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.airtel import AirtelUSSDTransport
from vumi.message import TransportUserMessage
from vumi.utils import http_request_full


class TestAirtelUSSDTransportTestCase(TransportTestCase):

    transport_class = AirtelUSSDTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestAirtelUSSDTransportTestCase, self).setUp()
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/airtel/ussd/',
            'airtel_username': 'userid',
            'airtel_password': 'password',
        }
        self.transport = yield self.get_transport(self.config)
        self.session_manager = self.transport.session_manager
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])
        yield self.session_manager.redis._purge_all()  # just in case

    def mk_full_request(self, **params):
        return http_request_full('%s?%s' % (self.transport_url,
            urlencode(params)), data='', method='GET')

    def mk_request(self, **params):
        defaults = {
            'userid': 'userid',
            'password': 'password',
            'MSISDN': '27761234567',
        }
        defaults.update(params)
        return self.mk_full_request(**defaults)

    def mk_ussd_request(self, content, **kwargs):
        defaults = {
            'MSC': 'msc',
            'input': content,
        }
        defaults.update(kwargs)
        return self.mk_request(**defaults)

    def mk_cleanup_request(self, **kwargs):
        defaults = {
            'clean': 'clean-session',
            'status': 522
        }
        defaults.update(kwargs)
        return self.mk_request(**defaults)

    @inlineCallbacks
    def test_inbound_begin(self):
        # Second connect is the actual start of the session
        deferred = self.mk_ussd_request('121')
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '')
        self.assertEqual(msg['to_addr'], '*121#')
        self.assertEqual(msg['from_addr'], '27761234567'),
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'airtel': {
                'MSC': 'msc',
            },
        })

        reply = TransportUserMessage(**msg.payload).reply("ussd message")
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response.delivered_body, 'ussd message')
        self.assertEqual(response.headers.getRawHeaders('Freeflow'), ['FC'])
        self.assertEqual(response.headers.getRawHeaders('charge'), ['N'])
        self.assertEqual(response.headers.getRawHeaders('amount'), ['0'])

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate prior BEG message
        yield self.session_manager.create_session('27761234567',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='*167*7*a*b',
                session_event=TransportUserMessage.SESSION_RESUME)

        # Safaricom gives us the history of the full session in the USSD_PARAMS
        # The last submitted bit of content is the last value delimited by '*'
        deferred = self.mk_ussd_request('167*7*a*b*c')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], 'c')
        self.assertEqual(msg['to_addr'], '*167*7#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

        reply = TransportUserMessage(**msg.payload).reply("hello world",
            continue_session=False)
        self.dispatch(reply)
        response = yield deferred
        self.assertEqual(response.delivered_body, 'hello world')
        self.assertEqual(response.headers.getRawHeaders('Freeflow'), ['FB'])

    @inlineCallbacks
    def test_inbound_resume_with_failed_to_addr_lookup(self):
        deferred = self.mk_full_request(MSISDN='123456',
            input='7*a', userid='userid', password='password')
        response = yield deferred
        self.assertEqual(json.loads(response.delivered_body), {
            'missing_parameter': ['MSC'],
        })

    @inlineCallbacks
    def test_to_addr_handling(self):
        defaults = {
            'MSISDN': '12345',
            'MSC': 'msc',
            'userid': 'userid',
            'password': 'password'
        }

        d1 = self.mk_full_request(input='167*7*1', **defaults)
        [msg1] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg1['to_addr'], '*167*7*1#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        reply = TransportUserMessage(**msg1.payload).reply("hello world",
            continue_session=True)
        yield self.dispatch(reply)
        yield d1

        # follow up with the user submitting 'a'
        d2 = self.mk_full_request(input='167*7*1*a', **defaults)
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
        d1 = self.mk_ussd_request('167*7*3')
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
        d2 = self.mk_ussd_request('167*7*3')
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
        yield self.session_manager.create_session('27761234567',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='*167*7*a*b')
        # we're submitting a bunch of *s
        deferred = self.mk_ussd_request('167*7*a*b*****')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '****')

        reply = TransportUserMessage(**msg.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield deferred
        session = yield self.session_manager.load_session('27761234567')
        self.assertEqual(session['last_ussd_params'], '*167*7*a*b*****')

    @inlineCallbacks
    def test_submitting_asterisks_as_values_after_asterisks(self):
        yield self.session_manager.create_session('27761234567',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='*167*7*a*b**')
        # we're submitting a bunch of *s
        deferred = self.mk_ussd_request('167*7*a*b*****')

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['content'], '**')

        reply = TransportUserMessage(**msg.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield deferred
        session = yield self.session_manager.load_session('27761234567')
        self.assertEqual(session['last_ussd_params'], '*167*7*a*b*****')

    @inlineCallbacks
    def test_submitting_with_base_code_empty_ussd_params(self):
        d1 = self.mk_ussd_request('167')
        [msg1] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg1['to_addr'], '*167#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        reply = TransportUserMessage(**msg1.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield d1

        # ask for first menu
        d2 = self.mk_ussd_request('167*1')
        [msg1, msg2] = yield self.wait_for_dispatched_messages(2)
        self.assertEqual(msg2['to_addr'], '*167#')
        self.assertEqual(msg2['content'], '1')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        reply = TransportUserMessage(**msg2.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield d2

        # ask for second menu
        d3 = self.mk_ussd_request('167*1*1')
        [msg1, msg2, msg3] = yield self.wait_for_dispatched_messages(3)
        self.assertEqual(msg3['to_addr'], '*167#')
        self.assertEqual(msg3['content'], '1')
        self.assertEqual(msg3['session_event'],
            TransportUserMessage.SESSION_RESUME)
        reply = TransportUserMessage(**msg3.payload).reply('Hello',
            continue_session=True)
        self.dispatch(reply)
        yield d3

    @inlineCallbacks
    def test_cleanup_unknown_session(self):
        response = yield self.mk_cleanup_request()
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, 'Unknown Session')

    @inlineCallbacks
    def test_cleanup_session(self):
        yield self.session_manager.create_session('27761234567',
            to_addr='*167*7#', from_addr='27761234567')
        response = yield self.mk_cleanup_request(MSISDN='27761234567')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '')
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['session_event'],
            TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(msg['to_addr'], '*167*7#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['transport_metadata'], {
            'airtel': {
                'status': '522',
                'clean': 'clean-session',
            }
            })

    @inlineCallbacks
    def test_cleanup_session_missing_params(self):
        response = yield self.mk_request(clean='clean-session')
        self.assertEqual(response.code, http.BAD_REQUEST)
        self.assertEqual(json.loads(response.delivered_body), {
            'missing_parameter': ['status'],
            })

    @inlineCallbacks
    def test_cleanup_session_invalid_auth(self):
        response = yield self.mk_cleanup_request(userid='foo', password='bar')
        self.assertEqual(response.code, http.FORBIDDEN)
        self.assertEqual(response.delivered_body, 'Forbidden')
