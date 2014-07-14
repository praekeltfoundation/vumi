import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.safaricom import SafaricomTransport
from vumi.transports.tests.helpers import TransportHelper
from vumi.utils import http_request


class TestSafaricomTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
            'web_port': 0,
            'web_path': '/api/v1/safaricom/ussd/',
        }
        self.tx_helper = self.add_helper(TransportHelper(SafaricomTransport))
        self.transport = yield self.tx_helper.get_transport(config)
        self.session_manager = self.transport.session_manager
        self.transport_url = self.transport.get_transport_url(
            config['web_path'])
        yield self.session_manager.redis._purge_all()  # just in case

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
        # Second connect is the actual start of the session
        deferred = self.mk_request(USSD_PARAMS='7')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
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

        yield self.tx_helper.make_dispatch_reply(msg, "ussd message")
        response = yield deferred
        self.assertEqual(response, 'CON ussd message')

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate prior BEG message
        yield self.session_manager.create_session('session-id',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='7*a*b',
                session_event=TransportUserMessage.SESSION_RESUME)

        # Safaricom gives us the history of the full session in the USSD_PARAMS
        # The last submitted bit of content is the last value delimited by '*'
        deferred = self.mk_request(USSD_PARAMS='7*a*b*c')

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
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

        yield self.tx_helper.make_dispatch_reply(
            msg, "hello world", continue_session=False)
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

        d1 = self.mk_full_request(USSD_PARAMS='7*1', **defaults)
        [msg1] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg1['to_addr'], '*167*7*1#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        yield self.tx_helper.make_dispatch_reply(msg1, "hello world")
        yield d1

        # follow up with the user submitting 'a'
        d2 = self.mk_full_request(USSD_PARAMS='7*1*a', **defaults)
        [msg1, msg2] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg2['to_addr'], '*167*7*1#')
        self.assertEqual(msg2['content'], 'a')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(
            msg2, "hello world", continue_session=False)
        yield d2

    @inlineCallbacks
    def test_hitting_url_twice_without_content(self):
        d1 = self.mk_request(USSD_PARAMS='7*3')
        [msg1] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg1['to_addr'], '*167*7*3#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        yield self.tx_helper.make_dispatch_reply(msg1, "Hello")
        yield d1

        # make the exact same request again
        d2 = self.mk_request(USSD_PARAMS='7*3')
        [msg1, msg2] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg2['to_addr'], '*167*7*3#')
        self.assertEqual(msg2['content'], '')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(msg2, "Hello")
        yield d2

    @inlineCallbacks
    def test_submitting_asterisks_as_values(self):
        yield self.session_manager.create_session('session-id',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='7*a*b')
        # we're submitting a bunch of *s
        deferred = self.mk_request(USSD_PARAMS='7*a*b*****')

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '****')

        yield self.tx_helper.make_dispatch_reply(msg, "Hello")
        yield deferred
        session = yield self.session_manager.load_session('session-id')
        self.assertEqual(session['last_ussd_params'], '7*a*b*****')

    @inlineCallbacks
    def test_submitting_asterisks_as_values_after_asterisks(self):
        yield self.session_manager.create_session('session-id',
                to_addr='*167*7#', from_addr='27761234567',
                last_ussd_params='7*a*b**')
        # we're submitting a bunch of *s
        deferred = self.mk_request(USSD_PARAMS='7*a*b*****')

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '**')

        yield self.tx_helper.make_dispatch_reply(msg, "Hello")
        yield deferred
        session = yield self.session_manager.load_session('session-id')
        self.assertEqual(session['last_ussd_params'], '7*a*b*****')

    @inlineCallbacks
    def test_submitting_with_base_code_empty_ussd_params(self):
        d1 = self.mk_request()
        [msg1] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg1['to_addr'], '*167#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        yield self.tx_helper.make_dispatch_reply(msg1, "Hello")
        yield d1

        # ask for first menu
        d2 = self.mk_request(USSD_PARAMS='1')
        [msg1, msg2] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg2['to_addr'], '*167#')
        self.assertEqual(msg2['content'], '1')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(msg2, "Hello")
        yield d2

        # ask for second menu
        d3 = self.mk_request(USSD_PARAMS='1*1')
        [m1, m2, msg3] = yield self.tx_helper.wait_for_dispatched_inbound(3)
        self.assertEqual(msg3['to_addr'], '*167#')
        self.assertEqual(msg3['content'], '1')
        self.assertEqual(msg3['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(msg3, "Hello")
        yield d3

    @inlineCallbacks
    def test_nack(self):
        msg = yield self.tx_helper.make_dispatch_outbound("outbound")
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
            'Missing fields: in_reply_to')
