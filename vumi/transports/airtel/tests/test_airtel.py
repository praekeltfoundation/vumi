import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.airtel import AirtelUSSDTransport
from vumi.message import TransportUserMessage
from vumi.utils import http_request_full
from vumi.transports.tests.helpers import TransportHelper


class AirtelUSSDTransportTestCase(VumiTestCase):

    airtel_username = None
    airtel_password = None
    session_id = 'session-id'

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(AirtelUSSDTransport))
        self.config = self.mk_config()
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.session_manager = self.transport.session_manager
        self.add_cleanup(self.session_manager.stop)
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])
        yield self.session_manager.redis._purge_all()  # just in case

    def mk_config(self):
        return {
            'web_port': 0,
            'web_path': '/api/v1/airtel/ussd/',
            'airtel_username': self.airtel_username,
            'airtel_password': self.airtel_password,
            'validation_mode': 'permissive',
        }

    def mk_full_request(self, **params):
        return http_request_full('%s?%s' % (self.transport_url,
            urlencode(params)), data='', method='GET')

    def mk_request(self, **params):
        defaults = {
            'MSISDN': '27761234567',
        }
        if all([self.airtel_username, self.airtel_password]):
            defaults.update({
                'userid': self.airtel_username,
                'password': self.airtel_password,
            })

        defaults.update(params)
        return self.mk_full_request(**defaults)

    def mk_ussd_request(self, content, **kwargs):
        defaults = {
            'MSC': 'msc',
            'input': content,
            'SessionID': self.session_id,
        }
        defaults.update(kwargs)
        return self.mk_request(**defaults)

    def mk_cleanup_request(self, **kwargs):
        defaults = {
            'clean': 'clean-session',
            'error': 522,
            'SessionID': self.session_id,
        }
        defaults.update(kwargs)
        return self.mk_request(**defaults)


class TestAirtelUSSDTransport(AirtelUSSDTransportTestCase):

    @inlineCallbacks
    def test_inbound_begin(self):
        # Second connect is the actual start of the session
        deferred = self.mk_ussd_request('121')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
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

        yield self.tx_helper.make_dispatch_reply(msg, "ussd message")
        response = yield deferred
        self.assertEqual(response.delivered_body, 'ussd message')
        self.assertEqual(response.headers.getRawHeaders('Freeflow'), ['FC'])
        self.assertEqual(response.headers.getRawHeaders('charge'), ['N'])
        self.assertEqual(response.headers.getRawHeaders('amount'), ['0'])

    @inlineCallbacks
    def test_strip_leading_newlines(self):
        # Second connect is the actual start of the session
        deferred = self.mk_ussd_request('121')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(msg, "\nfoo\n")
        response = yield deferred
        self.assertEqual(response.delivered_body, 'foo\n')
        self.assertEqual(response.headers.getRawHeaders('Freeflow'), ['FC'])
        self.assertEqual(response.headers.getRawHeaders('charge'), ['N'])
        self.assertEqual(response.headers.getRawHeaders('amount'), ['0'])

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        # first pre-populate the redis datastore to simulate prior BEG message
        yield self.session_manager.create_session(self.session_id,
                to_addr='*167*7#', from_addr='27761234567',
                session_event=TransportUserMessage.SESSION_RESUME)

        # Safaricom gives us the history of the full session in the USSD_PARAMS
        # The last submitted bit of content is the last value delimited by '*'
        deferred = self.mk_ussd_request('c')

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], 'c')
        self.assertEqual(msg['to_addr'], '*167*7#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

        yield self.tx_helper.make_dispatch_reply(
            msg, "hello world", continue_session=False)
        response = yield deferred
        self.assertEqual(response.delivered_body, 'hello world')
        self.assertEqual(response.headers.getRawHeaders('Freeflow'), ['FB'])

    @inlineCallbacks
    def test_inbound_resume_with_failed_to_addr_lookup(self):
        deferred = self.mk_request(MSISDN='123456',
                                   input='7*a', SessionID='foo')
        response = yield deferred
        self.assertEqual(json.loads(response.delivered_body), {
            'missing_parameter': ['MSC'],
        })

    @inlineCallbacks
    def test_to_addr_handling(self):
        d1 = self.mk_ussd_request('167*7*1')
        [msg1] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg1['to_addr'], '*167*7*1#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        yield self.tx_helper.make_dispatch_reply(msg1, "hello world")
        yield d1

        # follow up with the user submitting 'a'
        d2 = self.mk_ussd_request('a')
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
        d1 = self.mk_ussd_request('167*7*3')
        [msg1] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg1['to_addr'], '*167*7*3#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        yield self.tx_helper.make_dispatch_reply(msg1, 'Hello')
        yield d1

        # make the exact same request again
        d2 = self.mk_ussd_request('')
        [msg1, msg2] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg2['to_addr'], '*167*7*3#')
        self.assertEqual(msg2['content'], '')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(msg2, 'Hello')
        yield d2

    @inlineCallbacks
    def test_submitting_asterisks_as_values(self):
        yield self.session_manager.create_session(self.session_id,
                to_addr='*167*7#', from_addr='27761234567')
        # we're submitting a bunch of *s
        deferred = self.mk_ussd_request('****')

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '****')

        yield self.tx_helper.make_dispatch_reply(msg, 'Hello')
        yield deferred

    @inlineCallbacks
    def test_submitting_asterisks_as_values_after_asterisks(self):
        yield self.session_manager.create_session(self.session_id,
                to_addr='*167*7#', from_addr='27761234567')
        # we're submitting a bunch of *s
        deferred = self.mk_ussd_request('**')

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], '**')

        yield self.tx_helper.make_dispatch_reply(msg, 'Hello')
        yield deferred

    @inlineCallbacks
    def test_submitting_with_base_code_empty_ussd_params(self):
        d1 = self.mk_ussd_request('167')
        [msg1] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg1['to_addr'], '*167#')
        self.assertEqual(msg1['content'], '')
        self.assertEqual(msg1['session_event'],
            TransportUserMessage.SESSION_NEW)
        yield self.tx_helper.make_dispatch_reply(msg1, 'Hello')
        yield d1

        # ask for first menu
        d2 = self.mk_ussd_request('1')
        [msg1, msg2] = yield self.tx_helper.wait_for_dispatched_inbound(2)
        self.assertEqual(msg2['to_addr'], '*167#')
        self.assertEqual(msg2['content'], '1')
        self.assertEqual(msg2['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(msg2, 'Hello')
        yield d2

        # ask for second menu
        d3 = self.mk_ussd_request('1')
        [msg1, msg2, msg3] = (
            yield self.tx_helper.wait_for_dispatched_inbound(3))
        self.assertEqual(msg3['to_addr'], '*167#')
        self.assertEqual(msg3['content'], '1')
        self.assertEqual(msg3['session_event'],
            TransportUserMessage.SESSION_RESUME)
        yield self.tx_helper.make_dispatch_reply(msg3, 'Hello')
        yield d3

    @inlineCallbacks
    def test_cleanup_unknown_session(self):
        response = yield self.mk_cleanup_request(msisdn='foo')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, 'Unknown Session')

    @inlineCallbacks
    def test_cleanup_session(self):
        yield self.session_manager.create_session(self.session_id,
            to_addr='*167*7#', from_addr='27761234567')
        response = yield self.mk_cleanup_request(msisdn='27761234567')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['session_event'],
            TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(msg['to_addr'], '*167*7#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['transport_metadata'], {
            'airtel': {
                'error': '522',
                'clean': 'clean-session',
            }
            })

    @inlineCallbacks
    def test_cleanup_session_missing_params(self):
        response = yield self.mk_request(clean='clean-session')
        self.assertEqual(response.code, http.BAD_REQUEST)
        json_response = json.loads(response.delivered_body)
        self.assertEqual(set(json_response['missing_parameter']),
                         set(['msisdn', 'SessionID', 'error']))

    @inlineCallbacks
    def test_cleanup_as_seen_in_production(self):
        """what's a technical spec between friends?"""
        yield self.session_manager.create_session('13697502734175597',
            to_addr='*167*7#', from_addr='254XXXXXXXXX')
        query_string = ("msisdn=254XXXXXXXXX&clean=cleann&error=523"
                        "&SessionID=13697502734175597&MSC=254XXXXXXXXX"
                        "&=&=en&=9031510005344&=&=&=postpaid"
                        "&=20130528171235405&=200220130528171113956582")
        response = yield http_request_full(
            '%s?%s' % (self.transport_url, query_string),
            data='', method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(msg['to_addr'], '*167*7#')
        self.assertEqual(msg['from_addr'], '254XXXXXXXXX')
        self.assertEqual(msg['transport_metadata'], {
            'airtel': {
                'clean': 'cleann',
                'error': '523',
            }
        })


class TestAirtelUSSDTransportWithToAddrValidation(AirtelUSSDTransportTestCase):

    def mk_config(self):
        config = super(TestAirtelUSSDTransportWithToAddrValidation,
                       self).mk_config()
        config['to_addr_pattern'] = '^\*121#$'
        return config

    @inlineCallbacks
    def test_inbound_begin_with_valid_to_addr(self):
        # Second connect is the actual start of the session
        deferred = self.mk_ussd_request('121')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
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

        yield self.tx_helper.make_dispatch_reply(msg, "ussd message")
        response = yield deferred
        self.assertEqual(response.delivered_body, 'ussd message')
        self.assertEqual(response.headers.getRawHeaders('Freeflow'), ['FC'])
        self.assertEqual(response.headers.getRawHeaders('charge'), ['N'])
        self.assertEqual(response.headers.getRawHeaders('amount'), ['0'])

    @inlineCallbacks
    def test_inbound_begin_with_invalid_to_addr(self):
        # Second connect is the actual start of the session
        with LogCatcher(message='Unhappy') as lc:
            response = yield self.mk_ussd_request('123')
            [log_msg] = lc.messages()
        self.assertEqual(response.code, 400)
        error_msg = json.loads(response.delivered_body)
        expected_error = {
            'invalid_session': (
                "Session id u'session-id' has not been encountered in the"
                " last 600 seconds and the 'input' request parameter value"
                " u'*123#' doesn't look like a valid USSD address."
            )
        }
        self.assertEqual(error_msg, expected_error)
        self.assertEqual(
            log_msg, "Unhappy incoming message: %s" % (expected_error,))


class TestAirtelUSSDTransportWithAuth(TestAirtelUSSDTransport):

    transport_class = AirtelUSSDTransport
    airtel_username = 'userid'
    airtel_password = 'password'

    @inlineCallbacks
    def test_cleanup_session_invalid_auth(self):
        response = yield self.mk_cleanup_request(userid='foo', password='bar')
        self.assertEqual(response.code, http.FORBIDDEN)
        self.assertEqual(response.delivered_body, 'Forbidden')

    @inlineCallbacks
    def test_cleanup_as_seen_in_production(self):
        """what's a technical spec between friends?"""
        yield self.session_manager.create_session('13697502734175597',
            to_addr='*167*7#', from_addr='254XXXXXXXXX')
        query_string = ("msisdn=254XXXXXXXXX&clean=cleann&error=523"
                        "&SessionID=13697502734175597&MSC=254XXXXXXXXX"
                        "&=&=en&=9031510005344&=&=&=postpaid"
                        "&=20130528171235405&=200220130528171113956582"
                        "&userid=%s&password=%s" % (self.airtel_username,
                                                    self.airtel_password))
        response = yield http_request_full(
            '%s?%s' % (self.transport_url, query_string),
            data='', method='GET')
        self.assertEqual(response.code, http.OK)
        self.assertEqual(response.delivered_body, '')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(msg['to_addr'], '*167*7#')
        self.assertEqual(msg['from_addr'], '254XXXXXXXXX')
        self.assertEqual(msg['transport_metadata'], {
            'airtel': {
                'clean': 'cleann',
                'error': '523',
            }
        })


class TestLoadBalancedAirtelUSSDTransport(VumiTestCase):

    def setUp(self):
        self.default_config = {
            'web_port': 0,
            'web_path': '/api/v1/airtel/ussd/',
            'validation_mode': 'permissive',
        }
        self.tx_helper = self.add_helper(TransportHelper(AirtelUSSDTransport))

    @inlineCallbacks
    def test_session_prefixes(self):
        config1 = self.default_config.copy()
        config1['transport_name'] = 'transport_1'
        config1['session_key_prefix'] = 'foo'

        config2 = self.default_config.copy()
        config2['transport_name'] = 'transport_2'
        config2['session_key_prefix'] = 'foo'

        self.transport1 = yield self.tx_helper.get_transport(config1)
        self.transport2 = yield self.tx_helper.get_transport(config2)
        self.transport3 = yield self.tx_helper.get_transport(
            self.default_config)

        self.assertEqual(self.transport1.get_session_key_prefix(), 'foo')
        self.assertEqual(self.transport2.get_session_key_prefix(), 'foo')
        self.assertEqual(self.transport3.get_session_key_prefix(),
            'vumi.transports.airtel:sphex')
