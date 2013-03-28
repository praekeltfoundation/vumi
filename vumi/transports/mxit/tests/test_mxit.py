from twisted.internet.defer import inlineCallbacks
from twisted.web.http import Request

from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.mxit import MxitTransport
from vumi.message import TransportUserMessage
from vumi.utils import http_request


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
        self.transport = yield self.get_transport(self.config)

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
        self.assertEqual(self.transport.parse_profile(self.sample_profile_str),
            {
                'country_code': 'cc',
                'date_of_birth': 'dob',
                'gender': 'gender',
                'language_code': 'lc',
                'tariff_plan': 'tariff',

            })

    def test_html_decode(self):
        self.assertEqual(self.transport.html_decode(self.sample_html_str),
            '<&>')

    def test_get_request_data(self):
        req = Request(None, True)
        headers = req.requestHeaders
        headers.addRawHeader('X-Device-User-Agent', 'ua')
        headers.addRawHeader('X-Mxit-Contact', 'contact')
        headers.addRawHeader('X-Mxit-USERID-R', 'user-id')
        headers.addRawHeader('X-Mxit-Nick', 'nick')
        headers.addRawHeader('X-Mxit-Location', self.sample_loc_str)
        headers.addRawHeader('X-Mxit-Profile', self.sample_profile_str)
        headers.addRawHeader('X-Mxit-User-Input', self.sample_html_str)

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
