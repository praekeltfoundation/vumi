from base64 import b64decode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.tests.utils import MockHttpServer
from vumi.application.http_relay import HTTPRelayApplication
from vumi.message import TransportEvent

from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import VumiTestCase


class TestHTTPRelay(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        yield super(TestHTTPRelay, self).setUp()
        self.path = '/path'
        self.app_helper = self.add_helper(
            ApplicationHelper(HTTPRelayApplication))

    @inlineCallbacks
    def setup_resource_with_callback(self, callback):
        self.mock_server = MockHttpServer(callback)
        self.add_cleanup(self.mock_server.stop)
        yield self.mock_server.start()
        self.app = yield self.app_helper.get_application({
            'url': '%s%s' % (self.mock_server.url, self.path),
            'username': 'username',
            'password': 'password',
        })

    def setup_resource(self, code, content, headers):
        def handler(request):
            request.setResponseCode(code)
            for key, value in headers.items():
                request.setHeader(key, value)
            return content

        return self.setup_resource_with_callback(handler)

    @inlineCallbacks
    def test_http_relay_success_with_no_reply(self):
        yield self.setup_resource(http.OK, '', {})
        yield self.app_helper.make_dispatch_inbound("hi")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_http_relay_success_with_reply_header_true(self):
        yield self.setup_resource(http.OK, 'thanks!', {
            HTTPRelayApplication.reply_header: 'true',
        })
        msg = yield self.app_helper.make_dispatch_inbound("hi")
        [response] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(response['content'], 'thanks!')
        self.assertEqual(response['to_addr'], msg['from_addr'])

    @inlineCallbacks
    def test_http_relay_success_with_reply_header_false(self):
        yield self.setup_resource(http.OK, 'thanks!', {
            HTTPRelayApplication.reply_header: 'untrue!',
        })
        yield self.app_helper.make_dispatch_inbound("hi")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_http_relay_success_with_bad_reply(self):
        yield self.setup_resource(http.NOT_FOUND, '', {})
        yield self.app_helper.make_dispatch_inbound("hi")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_http_relay_success_with_bad_header(self):
        yield self.setup_resource(http.OK, 'thanks!', {
            'X-Other-Bad-Header': 'true',
        })
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_http_relay_with_basic_auth(self):
        def cb(request):
            headers = request.requestHeaders
            auth = headers.getRawHeaders('Authorization')[0]
            creds = auth.split(' ')[-1]
            username, password = b64decode(creds).split(':')
            self.assertEqual(username, 'username')
            self.assertEqual(password, 'password')
            request.setHeader(HTTPRelayApplication.reply_header, 'true')
            return 'thanks!'

        yield self.setup_resource_with_callback(cb)
        yield self.app_helper.make_dispatch_inbound("hi")
        [msg] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(msg['content'], 'thanks!')

    @inlineCallbacks
    def test_http_relay_with_bad_basic_auth(self):
        def cb(request):
            request.setResponseCode(http.UNAUTHORIZED)
            return 'Not Authorized'

        yield self.setup_resource_with_callback(cb)
        yield self.app_helper.make_dispatch_inbound("hi")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_http_relay_of_events(self):
        events = []

        def cb(request):
            events.append(TransportEvent.from_json(request.content.getvalue()))
            return ''

        yield self.setup_resource_with_callback(cb)
        dr = yield self.app_helper.make_dispatch_delivery_report()
        self.assertEqual([], self.app_helper.get_dispatched_outbound())
        self.assertEqual([dr], events)
