from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import NOT_DONE_YET
from twisted.web.client import Agent, ResponseDone

from vumi.transports.vumi_bridge.client import (
    StreamingClient, VumiBridgeInvalidJsonError)
from vumi.message import Message
from vumi.tests.fake_connection import FakeHttpServer
from vumi.tests.helpers import VumiTestCase


class TestStreamingClient(VumiTestCase):

    def setUp(self):
        self.fake_http = FakeHttpServer(self.handle_request)
        self.request_queue = DeferredQueue()
        self.client = StreamingClient(self.fake_http.get_agent)
        self.messages_received = DeferredQueue()
        self.errors_received = DeferredQueue()
        self.disconnects_received = DeferredQueue()

        def reason_trapper(reason):
            if reason.trap(ResponseDone):
                self.disconnects_received.put(reason.getErrorMessage())

        self.receiver = self.client.stream(
            Message,
            self.messages_received.put, self.errors_received.put,
            "http://vumi-go-api.example.com/", on_disconnect=reason_trapper)

    def handle_request(self, request):
        self.request_queue.put(request)
        return NOT_DONE_YET

    def test_default_agent_factory(self):
        """
        If `None` is passed as the `agent_factory`, `Agent` is used instead.
        """
        self.assertNotIsInstance(self.client.agent, Agent)
        self.assertIsInstance(StreamingClient(None).agent, Agent)

    @inlineCallbacks
    def test_callback_on_disconnect(self):
        req = yield self.request_queue.get()
        req.write(
            '%s\n' % (Message(foo='bar').to_json().encode('utf-8'),))
        req.finish()
        message = yield self.messages_received.get()
        self.assertEqual(message['foo'], 'bar')
        reason = yield self.disconnects_received.get()
        # this is the error message we get when a ResponseDone is raised
        # which happens when the remote server closes the connection.
        self.assertEqual(reason, 'Response body fully received')

    @inlineCallbacks
    def test_invalid_json(self):
        req = yield self.request_queue.get()
        req.write("Hello\n")
        req.finish()
        err = yield self.assertFailure(
            self.errors_received.get(), VumiBridgeInvalidJsonError)
        self.assertEqual(err.args, ("Hello",))
