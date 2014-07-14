# encoding: utf-8
from datetime import datetime
from urllib import urlencode

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.python import log
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.web.test.test_web import DummyRequest

from vumi.service import Worker
from vumi.transports.vas2nets.transport_stubs import (
    FakeVas2NetsHandler, FakeVas2NetsWorker)
from vumi.utils import StringProducer
from vumi.tests.helpers import VumiTestCase, WorkerHelper


def create_request(params={}, path='/', method='POST'):
    """
    Creates a dummy Vas2Nets request for testing our resources with
    """
    request = DummyRequest(path)
    request.method = method
    request.args = params
    return request


class TestResource(Resource):
    isLeaf = True

    def inc_receipts(self):
        self.receipts += 1

    def render_POST(self, request):
        log.msg(request.content.read())
        request.setResponseCode(http.OK)
        required_fields = ['smsid', 'status', 'text', 'time', 'provider',
                           'sender', 'messageid']
        log.msg('request.args', request.args)
        for key in required_fields:
            log.msg('checking for %s' % key)
            assert key in request.args

        self.inc_receipts()

        return ''


class TestWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        self.test_resource = TestResource()
        self.test_resource.receipts = 0
        self.resource = yield self.start_web_resources(
            [
                (self.test_resource, self.config['web_receipt_path']),
            ],
            self.config['web_port'],
        )

    def stopWorker(self):
        if hasattr(self, 'resource'):
            self.resource.stopListening()


class StubbedFakeVas2NetsWorker(FakeVas2NetsWorker):
    delay_choices = (0,)


class TestFakeVas2NetsWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.worker_helper = self.add_helper(WorkerHelper())
        self.config = {
            'web_port': 9999,
            'web_receive_path': '/t/receive',
            'web_receipt_path': '/t/receipt',
            'url': 'http://127.0.0.1:9998/t/send',
        }
        self.worker = yield self.worker_helper.get_worker(
            StubbedFakeVas2NetsWorker, self.config, start=False)
        self.test_worker = yield self.worker_helper.get_worker(
            TestWorker, self.config, start=False)
        self.today = datetime.utcnow().date()

    def render_request(self, resource, request):
        d = request.notifyFinish()
        response = resource.render(request)
        if response != NOT_DONE_YET:
            request.write(response)
            request.finish()
        return d

    @inlineCallbacks
    def test_receive_sent_sms(self):
        resource = FakeVas2NetsHandler('http://127.0.0.1:9999/t/receipt', (0,))
        resource.schedule_delivery = lambda *a: None

        request = create_request({
            'username': 'user',
            'password': 'pass',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice',
            'call-number': '+27831234567',
            'origin': '12345',
            'messageid': 'message_id',
            'provider': 'provider',
            'tariff': 0,
            'text': 'message content',
        })
        yield self.render_request(resource, request)
        self.assertEquals(''.join(request.written),
                          "Result_code: 00, Message OK")
        self.assertEquals(request.responseCode, http.OK)

    @inlineCallbacks
    def test_deliver_receipt(self):
        resource = FakeVas2NetsHandler('http://127.0.0.1:9999/t/receipt', (0,))
        yield self.test_worker.startWorker()

        yield resource.deliver_receipt('smsid', 'msgid', 'provider', 'sender')

    @inlineCallbacks
    def test_round_trip(self):
        d = Deferred()
        self.worker.deliver_hook = lambda x: d.callback(None)

        self.worker.startWorker()
        self.test_worker.startWorker()

        params = {
            'username': 'user',
            'password': 'pass',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice',
            'call-number': '+27831234567',
            'origin': '12345',
            'messageid': 'message_id',
            'provider': 'provider',
            'tariff': 0,
            'text': 'message content',
        }

        agent = Agent(reactor)
        response = yield agent.request('POST', self.config['url'],
            Headers({
                'User-Agent': ['Vumi Vas2Net Transport'],
                'Content-Type': ['application/x-www-form-urlencoded'],
            }),
            StringProducer(urlencode(params))
        )

        log.msg('Headers', list(response.headers.getAllRawHeaders()))
        self.assertTrue(response.headers.hasHeader('X-Nth-Smsid'))

        yield d
