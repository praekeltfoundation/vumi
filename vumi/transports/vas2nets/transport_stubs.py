# -*- test-case-name: vumi.transports.vas2nets.tests.test_vas2nets_stubs -*-

import uuid
import random
from urllib import urlencode
from StringIO import StringIO
from urlparse import urlparse
from datetime import datetime

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, Deferred, succeed
from twisted.internet import reactor
from twisted.internet.protocol import Protocol
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from vumi.utils import StringProducer
from vumi.service import Worker


class HttpResponseHandler(Protocol):
    def __init__(self, deferred):
        self.deferred = deferred
        self.stringio = StringIO()

    def dataReceived(self, bytes):
        self.stringio.write(bytes)

    def connectionLost(self, reason):
        self.deferred.callback(self.stringio.getvalue())

    @classmethod
    def handle(cls, response):
        deferred = Deferred()
        response.deliverBody(cls(deferred))
        return deferred

    @classmethod
    def req_POST(cls, url, params, headers=None):
        agent = Agent(reactor)
        hdrs = {
            'User-Agent': ['Vumi Vas2Net Faker'],
            'Content-Type': ['application/x-www-form-urlencoded'],
            }
        if headers:
            hdrs.update(headers)
        d = agent.request('POST', url, Headers(hdrs),
                          StringProducer(urlencode(params)))
        return d.addCallback(cls.handle)


class FakeVas2NetsHandler(Resource):
    """
    Resource to accept outgoing messages and reply with a delivery report.
    """
    isLeaf = True

    delay_choices = (0.5, 1, 1.5, 2, 5)
    deliver_hook = None

    def __init__(self, receipt_url, delay_choices=None, deliver_hook=None):
        if delay_choices:
            self.delay_choices = delay_choices
        if deliver_hook:
            self.deliver_hook = deliver_hook
        self.receipt_url = receipt_url

    def get_sms_id(self):
        return uuid.uuid4().get_hex()[-8:]

    def render_POST(self, request):
        request.setResponseCode(http.OK)
        required_fields = [
            'username', 'password', 'call-number', 'origin', 'text',
            'messageid', 'provider', 'tariff', 'owner', 'service',
            'subservice'
            ]
        log.msg('Received sms: %s' % (request.args,))
        for key in required_fields:
            if key not in request.args:
                request.setResponseCode(http.BAD_REQUEST)

        sms_id = self.get_sms_id()
        request.setHeader('X-Nth-Smsid', sms_id)
        self.schedule_delivery(sms_id, *[request.args.get(f) for f in
                                         ['messageid', 'provider', 'sender']])
        return "Result_code: 00, Message OK"

    def schedule_delivery(self, *args):
        if not self.receipt_url:
            return succeed(None)
        return reactor.callLater(random.choice(self.delay_choices),
                                 self.deliver_receipt, *args)

    def deliver_receipt(self, sms_id, message_id, provider, sender):
        CODES = [
            ('2', 'DELIVRD'),
            ('2', 'DELIVRD'),
            ('2', 'DELIVRD'),
            ('2', 'DELIVRD'),
            ('-28', 'Presumably failed.'),
            ]
        code, status = random.choice(CODES)
        params = {
            'smsid': sms_id,
            'messageid': message_id,
            'status': code,
            'text': status,
            'time': datetime.utcnow().strftime('%Y.%m.%d %H:%M:%S'),
            'provider': provider,
            'sender': sender,
            }

        log.msg("Sending receipt: %s" % (params,))
        d = HttpResponseHandler.req_POST(self.receipt_url, params)
        if self.deliver_hook:
            d.addCallback(self.deliver_hook)
        return d


class FakeVas2NetsWorker(Worker):
    delay_choices = None
    deliver_hook = None

    handler = FakeVas2NetsHandler

    @inlineCallbacks
    def startWorker(self):
        url = urlparse(self.config.get('url'))
        receipt_url = "http://127.0.0.1:%s%s" % (
            self.config.get('web_port'), self.config.get('web_receipt_path'))

        self.receipt_resource = yield self.start_web_resources(
            [(self.handler(receipt_url, self.delay_choices, self.deliver_hook),
              url.path)], url.port)

    def stopWorker(self):
        if hasattr(self, 'receipt_resource'):
            self.receipt_resource.stopListening()
