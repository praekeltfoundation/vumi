from twisted.internet.defer import inlineCallbacks
from twisted.web.http import Request, BAD_REQUEST

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.mxit import MxitTransport
from vumi.transports.mxit.responses import ResponseParser
from vumi.message import TransportUserMessage
from vumi.utils import http_request_full


class TestMxitTransportTestCase(TransportTestCase):

    transport_class = MxitTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestMxitTransportTestCase, self).setUp()
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/mxit/mobiportal/',
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

        self.transport = yield self.get_transport(self.config)
        self.url = self.transport.get_transport_url(self.config['web_path'])

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

    def test_get_request_content(self):
        req = Request(None, True)
        req.requestHeaders.addRawHeader('X-Mxit-User-Input', 'foo')
        self.assertEqual(self.transport.get_request_content(req), 'foo')
        req.args = {'input': ['bar']}
        self.assertEqual(self.transport.get_request_content(req), 'bar')

    @inlineCallbacks
    def test_invalid_request(self):
        resp = yield http_request_full(self.url)
        self.assertEqual(resp.code, BAD_REQUEST)

    @inlineCallbacks
    def test_request(self):
        resp_d = http_request_full(
            self.url, headers=self.sample_req_headers)
        [msg] = yield self.wait_for_dispatched_messages(1)
        reply = TransportUserMessage(**msg.payload).reply(
            self.sample_menu_resp, continue_session=True)
        self.dispatch(reply)
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
        [msg] = yield self.wait_for_dispatched_messages(1)
        reply = TransportUserMessage(**msg.payload).reply(
            self.sample_unicode_menu_resp, continue_session=True)
        self.dispatch(reply)
        resp = yield resp_d
        self.assertTrue(
            'Hell\xc3\xb8' in resp.delivered_body)
        self.assertTrue(
            '\xc3\xb8pti\xc3\xb8n 1' in resp.delivered_body)
