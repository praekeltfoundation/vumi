from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.web.client import Agent, readBody

from vumi.tests.utils import LogCatcher, MockHttpServer
from vumi import log
from vumi.tests.helpers import VumiTestCase


class TestLogCatcher(VumiTestCase):
    def test_simple_catching(self):
        lc = LogCatcher()
        with lc:
            log.info("Test")
        self.assertEqual(lc.messages(), ["Test"])

    def test_system_filtering(self):
        lc = LogCatcher(system="^ab")
        with lc:
            log.info("Test 1", system="abc")
            log.info("Test 2", system="def")
        self.assertEqual(lc.messages(), ["Test 1"])

    def test_message_filtering(self):
        lc = LogCatcher(message="^Keep")
        with lc:
            log.info("Keep this")
            log.info("Discard this")
        self.assertEqual(lc.messages(), ["Keep this"])

    def test_message_concatenation(self):
        lc = LogCatcher()
        with lc:
            log.info("Part 1", "Part 2")
        self.assertEqual(lc.messages(), ["Part 1 Part 2"])


class TestMockHttpServer(VumiTestCase):
    def start_mock_server(self, mock_server):
        """
        Start and return the given MockHttpServer with suitable cleanup.
        """
        self.add_cleanup(mock_server.stop)
        d = mock_server.start()
        return d.addCallback(lambda _: mock_server)

    @inlineCallbacks
    def assert_response(self, response, code, body=None):
        self.assertEqual(response.code, code)
        response_body = yield readBody(response)
        if body is not None:
            self.assertEqual(response_body, body)

    @inlineCallbacks
    def test_simple_GET(self):
        """
        MockHttpHelper can handle a simple HTTP GET request.
        """
        requests = []
        mock_server = yield self.start_mock_server(
            MockHttpServer(lambda req: requests.append(req) or "hi"))

        agent = Agent(reactor)
        response = yield agent.request("GET", mock_server.url + "path")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.path, "/path")
        yield self.assert_response(response, 200, "hi")

    @inlineCallbacks
    def test_simple_HEAD(self):
        """
        MockHttpHelper can handle a simple HTTP HEAD request.
        """
        requests = []
        mock_server = yield self.start_mock_server(
            MockHttpServer(lambda req: requests.append(req) or "hi"))

        agent = Agent(reactor)
        response = yield agent.request("HEAD", mock_server.url + "path")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "HEAD")
        self.assertEqual(request.path, "/path")
        yield self.assert_response(response, 200, "")

    @inlineCallbacks
    def test_simple_PUT(self):
        """
        MockHttpHelper can handle a simple HTTP PUT request.
        """
        requests = []
        mock_server = yield self.start_mock_server(
            MockHttpServer(lambda req: requests.append(req) or "hi"))

        agent = Agent(reactor)
        response = yield agent.request("PUT", mock_server.url + "path")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "PUT")
        self.assertEqual(request.path, "/path")
        yield self.assert_response(response, 200, "hi")

    @inlineCallbacks
    def test_simple_POST(self):
        """
        MockHttpHelper can handle a simple HTTP POST request.
        """
        requests = []
        mock_server = yield self.start_mock_server(
            MockHttpServer(lambda req: requests.append(req) or "hi"))

        agent = Agent(reactor)
        response = yield agent.request("POST", mock_server.url + "path")
        # We got a valid request and returned a valid response.
        [request] = requests
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.path, "/path")
        yield self.assert_response(response, 200, "hi")

    @inlineCallbacks
    def test_default_handler(self):
        """
        The default request handler puts the request in a queue.
        """
        mock_server = yield self.start_mock_server(MockHttpServer())

        agent = Agent(reactor)
        request_d = mock_server.queue.get()
        self.assertNoResult(request_d)
        response = yield agent.request("GET", mock_server.url + "path")
        # We got a valid request and returned a valid (error) response.
        request = self.successResultOf(request_d)
        self.assertEqual(request.method, "GET")
        self.assertEqual(request.path, "/path")
        yield self.assert_response(response, 500)
