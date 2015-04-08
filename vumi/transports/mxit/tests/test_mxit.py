import json
import base64

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.http import Request, BAD_REQUEST
from twisted.web.server import NOT_DONE_YET

from vumi.transports.mxit import MxitTransport
from vumi.transports.mxit.responses import ResponseParser
from vumi.utils import http_request_full
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper


class TestMxitTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.mock_http = MockHttpServer(self.handle_request)
        self.mock_request_queue = DeferredQueue()
        yield self.mock_http.start()
        self.addCleanup(self.mock_http.stop)

        config = {
            'web_port': 0,
            'web_path': '/api/v1/mxit/mobiportal/',
            'client_id': 'client_id',
            'client_secret': 'client_secret',
            'api_send_url': self.mock_http.url,
            'api_auth_url': self.mock_http.url,
        }
        self.sample_loc_str = 'cc,cn,sc,sn,cc,c,noi,cfb,ci'
        self.sample_profile_str = 'lc,cc,dob,gender,tariff'
        self.sample_html_str = '&lt;&amp;&gt;'
        self.sample_req_headers = {
            'X-Device-User-Agent': 'ua',
            'X-Mxit-Contact': 'contact',
            'X-Mxit-USERID-R': 'user-id',
            'X-Mxit-Nick': 'nick',
            'X-Mxit-Location': self.sample_loc_str,
            'X-Mxit-Profile': self.sample_profile_str,
            'X-Mxit-User-Input': self.sample_html_str,
        }
        self.sample_menu_resp = "\n".join([
            "Hello!",
            "1. option 1",
            "2. option 2",
            "3. option 3",
        ])
        # same as above but the o's are replaced with
        # http://www.fileformat.info/info/unicode/char/f8/index.htm
        slashed_o = '\xc3\xb8'
        self.sample_unicode_menu_resp = unicode(
            self.sample_menu_resp.replace('o', slashed_o), 'utf-8')

        self.tx_helper = self.add_helper(TransportHelper(MxitTransport))
        self.transport = yield self.tx_helper.get_transport(config)
        # NOTE: priming redis with an access token
        self.transport.redis.set(self.transport.access_token_key, 'foo')
        self.url = self.transport.get_transport_url(config['web_path'])

    def handle_request(self, request):
        self.mock_request_queue.put(request)
        return NOT_DONE_YET

    def test_is_mxit_request(self):
        req = Request(None, True)
        self.assertFalse(self.transport.is_mxit_request(req))
        req.requestHeaders.addRawHeader('X-Mxit-Contact', 'foo')
        self.assertTrue(self.transport.is_mxit_request(req))

    def test_noop(self):
        self.assertEqual(self.transport.noop('foo'), 'foo')

    def test_parse_location(self):
        self.assertEqual(self.transport.parse_location(self.sample_loc_str), {
            'country_code': 'cc',
            'country_name': 'cn',
            'subdivision_code': 'sc',
            'subdivision_name': 'sn',
            'city_code': 'cc',
            'city': 'c',
            'network_operator_id': 'noi',
            'client_features_bitset': 'cfb',
            'cell_id': 'ci',
        })

    def test_parse_profile(self):
        self.assertEqual(
            self.transport.parse_profile(self.sample_profile_str),
            {
                'country_code': 'cc',
                'date_of_birth': 'dob',
                'gender': 'gender',
                'language_code': 'lc',
                'tariff_plan': 'tariff',

            })

    def test_html_decode(self):
        self.assertEqual(
            self.transport.html_decode(self.sample_html_str), '<&>')

    def test_get_request_data(self):
        req = Request(None, True)
        headers = req.requestHeaders
        for key, value in self.sample_req_headers.items():
            headers.addRawHeader(key, value)

        data = self.transport.get_request_data(req)

        self.assertEqual(data, {
            'X-Device-User-Agent': 'ua',
            'X-Mxit-Contact': 'contact',
            'X-Mxit-Location': {
                'cell_id': 'ci',
                'city': 'c',
                'city_code': 'cc',
                'client_features_bitset': 'cfb',
                'country_code': 'cc',
                'country_name': 'cn',
                'network_operator_id': 'noi',
                'subdivision_code': 'sc',
                'subdivision_name': 'sn',
            },
            'X-Mxit-Nick': 'nick',
            'X-Mxit-Profile': {
                'country_code': 'cc',
                'date_of_birth': 'dob',
                'gender': 'gender',
                'language_code': 'lc',
                'tariff_plan': 'tariff',
            },
            'X-Mxit-USERID-R': 'user-id',
            'X-Mxit-User-Input': u'<&>',
        })

    def test_get_request_content_from_header(self):
        req = Request(None, True)
        req.requestHeaders.addRawHeader('X-Mxit-User-Input', 'foo')
        self.assertEqual(self.transport.get_request_content(req), 'foo')

    def test_get_quote_plus_request_content_from_header(self):
        req = Request(None, True)
        req.requestHeaders.addRawHeader('X-Mxit-User-Input', 'foo+bar')
        self.assertEqual(
            self.transport.get_request_content(req), 'foo bar')

    def test_get_quoted_request_content_from_header(self):
        req = Request(None, True)
        req.requestHeaders.addRawHeader('X-Mxit-User-Input', 'foo%20bar')
        self.assertEqual(
            self.transport.get_request_content(req), 'foo bar')

    def test_get_request_content_from_args(self):
        req = Request(None, True)
        req.args = {'input': ['bar']}
        self.assertEqual(self.transport.get_request_content(req), 'bar')

    def test_get_request_content_when_missing(self):
        req = Request(None, True)
        self.assertEqual(self.transport.get_request_content(req), None)

    @inlineCallbacks
    def test_invalid_request(self):
        resp = yield http_request_full(self.url)
        self.assertEqual(resp.code, BAD_REQUEST)

    @inlineCallbacks
    def test_request(self):
        resp_d = http_request_full(
            self.url, headers=self.sample_req_headers)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, self.sample_menu_resp)
        resp = yield resp_d
        self.assertTrue('1. option 1' in resp.delivered_body)
        self.assertTrue('2. option 2' in resp.delivered_body)
        self.assertTrue('3. option 3' in resp.delivered_body)

        self.assertTrue('?input=1' in resp.delivered_body)
        self.assertTrue('?input=2' in resp.delivered_body)
        self.assertTrue('?input=3' in resp.delivered_body)

    def test_response_parser(self):
        header, items = ResponseParser.parse(self.sample_menu_resp)
        self.assertEqual(header, 'Hello!')
        self.assertEqual(items, [
            ('1', 'option 1'),
            ('2', 'option 2'),
            ('3', 'option 3'),
        ])

        header, items = ResponseParser.parse('foo!')
        self.assertEqual(header, 'foo!')
        self.assertEqual(items, [])

    @inlineCallbacks
    def test_unicode_rendering(self):
        resp_d = http_request_full(
            self.url, headers=self.sample_req_headers)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, self.sample_unicode_menu_resp)
        resp = yield resp_d
        self.assertTrue(
            'Hell\xc3\xb8' in resp.delivered_body)
        self.assertTrue(
            '\xc3\xb8pti\xc3\xb8n 1' in resp.delivered_body)

    @inlineCallbacks
    def test_outbound_that_is_not_a_reply(self):
        d = self.tx_helper.make_dispatch_outbound(
            content="Send!", to_addr="mxit-1", from_addr="mxit-2")
        req = yield self.mock_request_queue.get()
        body = json.load(req.content)
        self.assertEqual(body, {
            'Body': 'Send!',
            'To': 'mxit-1',
            'From': 'mxit-2',
            'ContainsMarkup': 'true',
            'Spool': 'true',
        })
        [auth] = req.requestHeaders.getRawHeaders('Authorization')
        # primed access token
        self.assertEqual(auth, 'Bearer foo')
        req.finish()

        yield d

    @inlineCallbacks
    def test_getting_access_token(self):
        transport = self.transport
        redis = transport.redis
        # clear primed value
        yield redis.delete(transport.access_token_key)

        d = transport.get_access_token()

        req = yield self.mock_request_queue.get()
        [auth] = req.requestHeaders.getRawHeaders('Authorization')
        self.assertEqual(
            auth, 'Basic %s' % (
                base64.b64encode('client_id:client_secret')))
        self.assertEqual(
            ['grant_type=client_credentials', 'scope=message%2Fsend'],
            sorted(req.content.read().split('&')))
        req.write(json.dumps({
            'access_token': 'access_token',
            'expires_in': '10'
        }))
        req.finish()

        access_token = yield d
        self.assertEqual(access_token, 'access_token')
        self.assertFalse(isinstance(access_token, unicode))
        ttl = yield redis.ttl(transport.access_token_key)
        self.assertTrue(
            0 < ttl <= (transport.access_token_auto_decay * 10))
