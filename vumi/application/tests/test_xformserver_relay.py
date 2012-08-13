from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.tests.utils import TestResourceWorker, get_stubbed_worker
from vumi.application.tests.test_xformserver_relay_stubs import TestXformServer
from vumi.application.xformserver_relay import XformServerRelayApplication
from vumi.message import TransportEvent
from base64 import b64decode
import json


class XformServerRelayTestCase(ApplicationTestCase):

    application_class = XformServerRelayApplication
    timeout = 1

    @inlineCallbacks
    def setUp(self):
        yield super(XformServerRelayTestCase, self).setUp()
        self.path = ''

    #def _persist_tearDown(self):
        #pass

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
                (self.path, TestXformServer, (code, content, headers, callback))])
        self._workers.append(w)
        yield w.startWorker()
        returnValue(w)

    @inlineCallbacks
    def test_xformserver_relay_new_form(self):
        form = open("../vumi/application/tests/xform.xml", 'r')
        form_content = form.read()
        expected = {
                u'seq_id': 0,
                u'langs': [u'default'],
                u'event': {
                    u'ix': u'0',
                    u'style': {},
                    u'help': None,
                    u'datatype': u'select',
                    u'required': 0,
                    u'choices': [u'Texas', u'Washington'],
                    u'caption': u'Please select a State',
                    u'answer': None,
                    u'type': u'question'
                    },
                u'session_id': 1,
                u'title': u'cascading select test'
                }
        yield self.setup_resource(http.OK, json.dumps(expected), {
            XformServerRelayApplication.reply_header: 'true',
        })
        msg = self.mkmsg_in(content="1",
                            helper_metadata={'form_content': form_content})
        #print msg.payload
        yield self.dispatch(msg)
        [response] = self.get_dispatched_messages()
        self.assertEqual(response['content'], json.dumps(expected))
        self.assertEqual(response['to_addr'], msg['from_addr'])

    def test_xformserver_relay_answer(self):
        expected = {
                }
        yield self.setup_resource(http.OK, json.dumps(expected), {
            XformServerRelayApplication.reply_header: 'true',
        })
        msg = self.mkmsg_in(content="1")
        #print msg.payload
        yield self.dispatch(msg)
        [response] = self.get_dispatched_messages()
        self.assertEqual(response['content'], json.dumps(expected))
        self.assertEqual(response['to_addr'], msg['from_addr'])
