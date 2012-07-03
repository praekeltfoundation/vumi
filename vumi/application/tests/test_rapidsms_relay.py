"""Tests for vumi.application.rapidsms_relay."""

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.tests.utils import TestResourceWorker, get_stubbed_worker
from vumi.application.tests.test_http_relay_stubs import TestResource
from vumi.application.rapidsms_relay import RapidSMSRelay


class RapidSMSRelayTestCase(ApplicationTestCase):

    application_class = RapidSMSRelay
    timeout = 5

    @inlineCallbacks
    def setUp(self):
        yield super(RapidSMSRelayTestCase, self).setUp()
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
            'rapidsms_url': 'http://localhost:%s%s' % (resource.port, path),
            'web_path': '/send/',
            'web_port': '0',
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
    def test_rapidsms_relay_success(self):
        yield self.setup_resource(http.OK, '', {})
        msg = self.mkmsg_in()
        yield self.dispatch(msg)
        self.assertEqual([], self.get_dispatched_messages())
