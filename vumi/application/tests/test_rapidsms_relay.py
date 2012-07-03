"""Tests for vumi.application.rapidsms_relay."""

from base64 import b64decode

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http
from twisted.web.resource import Resource

from vumi.application.tests.test_base import ApplicationTestCase
from vumi.tests.utils import TestResourceWorker, LogCatcher, get_stubbed_worker
from vumi.application.rapidsms_relay import RapidSMSRelay


class TestResource(Resource):
    isLeaf = True

    def __init__(self, callback):
        self.callback = callback

    def render_POST(self, request):
        return self.callback(request)

        request.setResponseCode(self.code)
        for key, value in self.headers.items():
            request.setHeader(key, value)
        return self.content


class RapidSMSRelayTestCase(ApplicationTestCase):

    application_class = RapidSMSRelay
    timeout = 5
    path = '/test/resource/path'

    @inlineCallbacks
    def setUp(self):
        yield super(RapidSMSRelayTestCase, self).setUp()

    @inlineCallbacks
    def setup_resource(self, callback):
        self.resource = yield self.make_resource_worker(callback=callback)
        self.app = yield self.setup_app(self.path, self.resource)

    @inlineCallbacks
    def setup_app(self, path, resource):
        app = yield self.get_application({
            'rapidsms_url': 'http://localhost:%s%s' % (resource.port, path),
            'web_path': '/send/',
            'web_port': '0',
            'username': 'username',
            'password': 'password',
        })
        returnValue(app)

    @inlineCallbacks
    def make_resource_worker(self, callback):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([(self.path, TestResource, (callback,))])
        self._workers.append(w)
        yield w.startWorker()
        returnValue(w)

    @inlineCallbacks
    def test_rapidsms_relay_success(self):
        def cb(request):
            self.assertEqual(request.args['sms'], ['hello world'])
            self.assertEqual(request.args['sender'], ['+41791234567'])
            return 'OK'

        yield self.setup_resource(cb)
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], self.get_dispatched_messages())

    @inlineCallbacks
    def test_rapidsms_relay_with_basic_auth(self):
        def cb(request):
            headers = request.requestHeaders
            auth = headers.getRawHeaders('Authorization')[0]
            creds = auth.split(' ')[-1]
            username, password = b64decode(creds).split(':')
            self.assertEqual(username, 'username')
            self.assertEqual(password, 'password')
            self.assertEqual(request.args['sms'], ['hello world'])
            self.assertEqual(request.args['sender'], ['+41791234567'])
            return 'OK'

        yield self.setup_resource(cb)
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], self.get_dispatched_messages())

    @inlineCallbacks
    def test_rapidsms_relay_with_bad_basic_auth(self):
        def cb(request):
            request.setResponseCode(http.UNAUTHORIZED)
            return 'Not Authorized'

        yield self.setup_resource(cb)
        yield self.dispatch(self.mkmsg_in())
        self.assertEqual([], self.get_dispatched_messages())

    @inlineCallbacks
    def test_rapidsms_relay_logs_events(self):
        def cb(request):
            self.fail("RapidSMS relay should not send events to RapidSMS")

        yield self.setup_resource(cb)
        with LogCatcher() as lc:
            yield self.dispatch(self.mkmsg_delivery(), rkey=self.rkey('event'))
            yield self.dispatch(self.mkmsg_ack(), rkey=self.rkey('event'))
            self.assertEqual(lc.messages(), [
                "Delivery report received for message u'abc',"
                " status u'delivered'",
                "Acknowledgement received for message u'1'",
            ])
        self.assertEqual([], self.get_dispatched_messages())
