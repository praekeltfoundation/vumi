from uuid import uuid4
from datetime import datetime

from twisted.internet.defer import inlineCallbacks
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.yo_ug_http.yo_ug_http import YoUgHttpTransport
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import from_json

class YoUgHttpTransportTestCase(TransportTestCase):

    transport_name = 'yo'
    transport_type = 'sms'
    transport_class = YoUgHttpTransport

    @inlineCallbacks
    def setUp(self):
        yield super(YoUgHttpTransportTestCase, self).setUp()
        self.path = '/sendsms'
        self.port = 9999
        self.config = {
            'transport_name': 'yo',
            'url':'http://localhost:%s%s'%(self.port, self.path),
            'ybsacctno':'ybsacctno',
            'password':'password',          
        }
        self.worker = yield self.get_transport(self.config)
        self.today = datetime.utcnow().date()

    def make_resource_worker(self, msg, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
            (self.path, TestResource, ( msg, code, send_id))])
        self._workers.append(w)
        return w.startWorker()
    
    @inlineCallbacks
    def test_sending_one_sms_ok(self):
        #mocked_message_id = str(uuid4())
        mocked_message = "ybs_autocreate_status=OK"
        #HTTP response
        yield self.make_resource_worker(mocked_message) 
        #Message to transport
        yield self.dispatch(self.mkmsg_out())    
        [smsg] = self.get_dispatched('yo.event')
        self.assertEqual(self.mkmsg_ack(),
                         from_json(smsg.body))

    @inlineCallbacks
    def test_receiving_one_sms(self):
        self.assertTrue(False)
    
    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)


class TestResource(Resource):
    isLeaf = True
    
    def __init__(self, message, code= http.OK, send_id=None):
        self.message = message
        self.code = code
        self.send_id = send_id

    def render_GET(self, request):
        #log.msg(request.content.read())
        request.setResponseCode(self.code)
        #log.msg('request.arg', request.args)
        return self.message
    

