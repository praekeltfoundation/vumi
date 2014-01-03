# -*- test-case-name: vumi.transports.vumi_bridge.tests.test_client -*-
import json

from twisted.internet.defer import Deferred
from twisted.internet import reactor
from twisted.web.client import Agent, ResponseDone, ResponseFailed
from twisted.web import http
from twisted.protocols import basic
from twisted.python.failure import Failure

from vumi.message import Message
from vumi.utils import to_kwargs
from vumi import log
from vumi.errors import VumiError


class VumiBridgeError(VumiError):
    """Raised by errors encountered by VumiBridge."""


class VumiBridgeInvalidJsonError(VumiError):
    """Raised when invalid JSON is received."""


class VumiMessageReceiver(basic.LineReceiver):

    delimiter = '\n'
    message_class = Message

    def __init__(self, message_class, callback, errback, on_connect=None,
                 on_disconnect=None):
        self.message_class = message_class
        self.callback = callback
        self.errback = errback
        self._response = None
        self._wait_for_response = Deferred()
        self._on_connect = on_connect or (lambda *a: None)
        self._on_disconnect = on_disconnect or (lambda *a: None)
        self.disconnecting = False

    def get_response(self):
        return self._wait_for_response

    def handle_response(self, response):
        self._response = response
        if self._response.code == http.NO_CONTENT:
            self._wait_for_response.callback(self._response)
        else:
            self._response.deliverBody(self)

    def lineReceived(self, line):
        d = Deferred()
        d.addCallback(self.callback)
        d.addErrback(self.errback)
        line = line.strip()
        try:
            data = json.loads(line)
            d.callback(self.message_class(
                _process_fields=True, **to_kwargs(data)))
        except ValueError, e:
            f = Failure(VumiBridgeInvalidJsonError(line))
            d.errback(f)
        except Exception, e:
            log.err()
            f = Failure(e)
            d.errback(f)

    def connectionMade(self):
        self._on_connect()

    def connectionLost(self, reason):
        # the PotentialDataLoss here is because Twisted didn't receive a
        # content length header, which is normal because we're streaming.
        if (reason.check(ResponseDone, ResponseFailed, http.PotentialDataLoss)
                and self._response is not None
                and not self._wait_for_response.called):
            self._wait_for_response.callback(self._response)
        if not self.disconnecting:
            self._on_disconnect(reason)

    def disconnect(self):
        self.disconnecting = True
        if self.transport and self.transport._producer is not None:
            self.transport._producer.loseConnection()
            self.transport._stopProxying()


class StreamingClient(object):

    def __init__(self):
        self.agent = Agent(reactor)

    def stream(self, message_class, callback, errback, url,
               headers=None, on_connect=None, on_disconnect=None):
        receiver = VumiMessageReceiver(
            message_class, callback, errback,
            on_connect=on_connect,
            on_disconnect=on_disconnect)
        d = self.agent.request('GET', url, headers)
        d.addCallback(lambda response: receiver.handle_response(response))
        d.addErrback(log.err)
        return receiver
