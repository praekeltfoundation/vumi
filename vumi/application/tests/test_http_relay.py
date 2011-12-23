from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.tests.utils import TestResourceWorker, get_stubbed_worker
from vumi.application.tests.test_http_relay_stubs import TestResource
from vumi.application.http_relay import HTTPRelayApplication
from base64 import b64decode


class HTTPRelayTestCase(ApplicationTestCase):

    application_class = HTTPRelayApplication

    @inlineCallbacks
    def setUp(self):
        yield super(HTTPRelayTestCase, self).setUp()
        self.path = '/path'

    @inlineCallbacks
    def setup_resource_with_callback(self, callback):
        self.resource = yield self.make_resource_worker(callback=callback)
        self.app = yield self.setup_app(self.path, self.resource)

    @inlineCallbacks
    def setup_resource(self, code, content, headers):
        self.resource = yield self.make_resource_worker(code, content,
                                                        headers)
        self.app = yield self.setup_app(self.path, self.resource)

    @inlineCallbacks
    def setup_app(self, path, resource):
        app = yield self.get_application({
            'url': 'http://localhost:%s%s' % (
                resource.port,
                path),
            'username': 'username',
            'password': 'password',
        })
        returnValue(app)

    @inlineCallbacks
    def make_resource_worker(self, code=http.OK, content='',
                                headers={}, callback=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
                (self.path, TestResource, (code, content, headers, callback))])
        self._workers.append(w)
        yield w.startWorker()
        returnValue(w)

    @inlineCallbacks
    def test_http_relay_success_with_no_reply(self):
        yield self.setup_resource(http.OK, '', {})
        msg = self.mkmsg_in()
        yield self.dispatch(msg)
        self.assertEqual([], self.get_dispatched_messages())

    @inlineCallbacks
    def test_http_relay_success_with_reply_header_true(self):
        yield self.setup_resource(http.OK, 'thanks!', {
            HTTPRelayApplication.reply_header: 'true',
        })
        msg = self.mkmsg_in()
        yield self.dispatch(msg)
        [response] = self.get_dispatched_messages()
        self.assertEqual(response['content'], 'thanks!')
        self.assertEqual(response['to_addr'], msg['from_addr'])

    @inlineCallbacks
    def test_http_relay_success_with_reply_header_false(self):
        yield self.setup_resource(http.OK, 'thanks!', {
            HTTPRelayApplication.reply_header: 'untrue!',
        })
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], self.get_dispatched_messages())

    @inlineCallbacks
    def test_http_relay_success_with_bad_reply(self):
        yield self.setup_resource(http.NOT_FOUND, '', {})
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], self.get_dispatched_messages())

    @inlineCallbacks
    def test_http_relay_success_with_bad_header(self):
        yield self.setup_resource(http.OK, 'thanks!', {
            'X-Other-Bad-Header': 'true',
        })
        self.assertEqual([], self.get_dispatched_messages())

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
        yield self.dispatch(self.mkmsg_in())
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['content'], 'thanks!')

    @inlineCallbacks
    def test_http_relay_with_bad_basic_auth(self):
        def cb(request):
            request.setResponseCode(http.UNAUTHORIZED)
            return 'Not Authorized'

        yield self.setup_resource_with_callback(cb)
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], self.get_dispatched_messages())
