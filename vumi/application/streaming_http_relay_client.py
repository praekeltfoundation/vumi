# -*- test-case-name: vumi.application.tests.test_streaming_http_relay -*-

import json

from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.web.client import Agent, ResponseDone, ResponseFailed
from twisted.web import http
from twisted.protocols import basic

from vumi.utils import to_kwargs
from vumi.message import Message
from vumi import log


class VumiMessageReceiver(basic.LineReceiver):

    delimiter = '\n'
    message_class = Message

    def __init__(self, message_class, callback, errback):
        self.message_class = message_class
        self.callback = callback
        self.errback = errback
        self._response = None
        self._wait_for_response = Deferred()

    def get_response(self):
        return self._wait_for_response

    def handle_response(self, response):
        self._response = response
        if self._response.code == http.NO_CONTENT:
            self._wait_for_response.callback(self._response)
        else:
            self._response.deliverBody(self)

    def lineReceived(self, line):
        line = line.strip()
        try:
            data = json.loads(line)
            self.callback(self.message_class(
                _process_fields=True, **to_kwargs(data)))
        except Exception, e:
            self.errback(e)

    def connectionLost(self, reason):
        # the PotentialDataLoss here is because Twisted didn't receive a
        # content length header, which is normal because we're streaming.
        if (reason.check(ResponseDone, ResponseFailed, http.PotentialDataLoss)
                and self._response is not None
                and not self._wait_for_response.called):
            self._wait_for_response.callback(self._response)
        else:
            self.errback(reason)

    def disconnect(self):
        if self.transport and self.transport._producer is not None:
            self.transport._producer.loseConnection()
            self.transport._stopProxying()


class StreamingClient(object):

    def __init__(self):
        self.agent = Agent(reactor)

    def stream(self, message_class, callback, errback, url, headers=None):
        receiver = VumiMessageReceiver(message_class, callback, errback)
        d = self.agent.request('GET', url, headers)
        d.addCallback(lambda response: receiver.handle_response(response))
        d.addErrback(log.err)
        return receiver
