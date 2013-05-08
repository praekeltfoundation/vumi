import base64
import json

from twisted.internet.defer import inlineCallbacks, DeferredQueue, returnValue
from twisted.web.http_headers import Headers
from twisted.web import http
from twisted.web.server import NOT_DONE_YET

from vumi.utils import http_request_full
from vumi.message import TransportUserMessage, TransportEvent
from vumi.tests.utils import MockHttpServer
from vumi.application.tests.utils import ApplicationTestCase
from vumi.application.streaming_http_relay import (
    StreamingHTTPRelayWorker, APIUserResource, StreamResource)
from vumi.application.streaming_http_relay_client import (
    StreamingClient, VumiMessageReceiver)


class TestMessageReceiver(VumiMessageReceiver):
    message_class = TransportUserMessage

    def __init__(self, *args, **kwargs):
        VumiMessageReceiver.__init__(self, *args, **kwargs)
        self.inbox = DeferredQueue()
        self.errors = DeferredQueue()

    def onMessage(self, message):
        self.inbox.put(message)

    def onError(self, failure):
        self.errors.put(failure)


class TestEventReceiver(TestMessageReceiver):
    message_class = TransportEvent


class StreamingHTTPWorkerTestCase(ApplicationTestCase):

    use_riak = True
    application_class = StreamingHTTPRelayWorker

    def mk_app_config(self):
        self.api_username = 'test_api_username'
        return self.mk_config({
            'worker_name': 'worker_name',
            'health_path': '/health/',
            'web_path': '/foo',
            'web_port': 0,
            'metrics_prefix': 'metrics_prefix.',
            'api_username': self.api_username,
            'api_auth_tokens': [
                'token-1',
                'token-2',
                'token-3',
            ],
        })

    @inlineCallbacks
    def setUp(self):
        yield super(StreamingHTTPWorkerTestCase, self).setUp()
        self.config = self.mk_app_config()
        self.app = yield self.get_application(self.config)
        self.addr = self.app.webserver.getHost()
        self.url = 'http://%s:%s%s' % (self.addr.host, self.addr.port,
                                        self.config['web_path'])

        self.auth_headers = {
            'Authorization': ['Basic ' + base64.b64encode('%s:%s' % (
                self.api_username, 'token-1'))],
        }

        self.client = StreamingClient()

        # Mock server to test HTTP posting of inbound messages & events
        self.mock_push_server = MockHttpServer(self.handle_request)
        yield self.mock_push_server.start()
        self.push_calls = DeferredQueue()

    @inlineCallbacks
    def tearDown(self):
        yield self.mock_push_server.stop()
        yield super(StreamingHTTPWorkerTestCase, self).tearDown()

    def handle_request(self, request):
        self.push_calls.put(request)
        return NOT_DONE_YET

    @inlineCallbacks
    def pull_message(self, count=1):
        url = '%s/%s/messages.json' % (self.url, self.api_username)

        messages = DeferredQueue()
        errors = DeferredQueue()
        receiver = self.client.stream(
            TransportUserMessage, messages.put, errors.put, url,
            Headers(self.auth_headers))

        received_messages = []
        for msg_id in range(count):
            sent_msg = self.mkmsg_in(content='in %s' % (msg_id,),
                                     message_id=str(msg_id))
            yield self.dispatch(sent_msg)
            recv_msg = yield messages.get()
            received_messages.append(recv_msg)

        receiver.disconnect()
        returnValue((receiver, received_messages))

    @inlineCallbacks
    def test_proxy_buffering_headers_off(self):
        receiver, received_messages = yield self.pull_message()
        headers = receiver._response.headers
        self.assertEqual(headers.getRawHeaders('x-accel-buffering'), ['no'])

    @inlineCallbacks
    def test_proxy_buffering_headers_on(self):
        StreamResource.proxy_buffering = True
        receiver, received_messages = yield self.pull_message()
        headers = receiver._response.headers
        self.assertEqual(headers.getRawHeaders('x-accel-buffering'), ['yes'])

    @inlineCallbacks
    def test_messages_stream(self):
        url = '%s/%s/messages.json' % (self.url, self.api_username)

        messages = DeferredQueue()
        errors = DeferredQueue()
        receiver = self.client.stream(
            TransportUserMessage, messages.put, errors.put, url,
            Headers(self.auth_headers))

        msg1 = self.mkmsg_in(content='in 1', message_id='1')
        yield self.dispatch(msg1)

        msg2 = self.mkmsg_in(content='in 2', message_id='2')
        yield self.dispatch(msg2)

        rm1 = yield messages.get()
        rm2 = yield messages.get()

        receiver.disconnect()

        self.assertEqual(msg1['message_id'], rm1['message_id'])
        self.assertEqual(msg2['message_id'], rm2['message_id'])
        self.assertEqual(errors.size, None)

    @inlineCallbacks
    def test_events_stream(self):
        url = '%s/%s/events.json' % (self.url, self.api_username)

        events = DeferredQueue()
        errors = DeferredQueue()
        receiver = yield self.client.stream(
            TransportEvent, events.put, events.put, url,
            Headers(self.auth_headers))

        ack1 = self.mkmsg_ack(user_message_id='message_id_1')
        yield self.dispatch_event(ack1)

        ack2 = self.mkmsg_ack(user_message_id='message_id_2')
        yield self.dispatch_event(ack2)

        ra1 = yield events.get()
        ra2 = yield events.get()

        receiver.disconnect()

        self.assertEqual(ack1['event_id'], ra1['event_id'])
        self.assertEqual(ack2['event_id'], ra2['event_id'])
        self.assertEqual(errors.size, None)

    @inlineCallbacks
    def test_missing_auth(self):
        url = '%s/%s/messages.json' % (self.url, self.api_username)

        queue = DeferredQueue()
        receiver = self.client.stream(
            TransportUserMessage, queue.put, queue.put, url)
        response = yield receiver.get_response()
        self.assertEqual(response.code, http.UNAUTHORIZED)
        self.assertEqual(response.headers.getRawHeaders('www-authenticate'), [
            'basic realm="Vumi Message Stream"'])

    @inlineCallbacks
    def test_invalid_auth(self):
        url = '%s/%s/messages.json' % (self.url, self.api_username)

        queue = DeferredQueue()

        headers = Headers({
            'Authorization': ['Basic %s' % (base64.b64encode('foo:bar'),)],
        })

        receiver = self.client.stream(
            TransportUserMessage, queue.put, queue.put, url, headers)
        response = yield receiver.get_response()
        self.assertEqual(response.code, http.UNAUTHORIZED)
        self.assertEqual(response.headers.getRawHeaders('www-authenticate'), [
            'basic realm="Vumi Message Stream"'])

    @inlineCallbacks
    def test_send_to(self):
        msg = {
            'to_addr': '+2345',
            'content': 'foo',
            'message_id': 'evil_id',
        }

        # TaggingMiddleware.add_tag_to_msg(msg, self.tag)

        url = '%s/%s/messages.json' % (self.url, self.api_username)
        response = yield http_request_full(url, json.dumps(msg),
                                           self.auth_headers, method='PUT')

        self.assertEqual(response.code, http.OK)
        put_msg = json.loads(response.delivered_body)

        [sent_msg] = self.get_dispatched_messages()
        self.assertEqual(sent_msg['to_addr'], sent_msg['to_addr'])
        # We do not respect the message_id that's been given.
        self.assertNotEqual(sent_msg['message_id'], msg['message_id'])
        self.assertEqual(sent_msg['message_id'], put_msg['message_id'])
        self.assertEqual(sent_msg['to_addr'], msg['to_addr'])
        self.assertEqual(sent_msg['from_addr'], None)

    @inlineCallbacks
    def test_invalid_reply(self):
        msg = {
            'to_addr': '+2345',
            'content': 'foo',
            'in_reply_to': '123',
            # No 'transport_metadata' field.
        }
        url = '%s/%s/messages.json' % (self.url, self.api_username)
        response = yield http_request_full(url, json.dumps(msg),
                                           self.auth_headers, method='PUT')
        self.assertEqual(response.code, http.BAD_REQUEST)

    @inlineCallbacks
    def test_reply(self):
        msg = {
            'to_addr': '+2345',
            'content': 'foo',
            'in_reply_to': '123',
            'transport_metadata': {
                'foo': 'bar',
            },
        }
        url = '%s/%s/messages.json' % (self.url, self.api_username)
        response = yield http_request_full(url, json.dumps(msg),
                                           self.auth_headers, method='PUT')

        put_msg = json.loads(response.delivered_body)
        self.assertEqual(response.code, http.OK)

        [sent_msg] = self.get_dispatched_messages()
        self.assertEqual(sent_msg['to_addr'], put_msg['to_addr'])
        self.assertEqual(sent_msg['transport_metadata'], {'foo': 'bar'})
        self.assertEqual(sent_msg['in_reply_to'], msg['in_reply_to'])
        self.assertEqual(sent_msg['message_id'], put_msg['message_id'])

    @inlineCallbacks
    def test_concurrency_limits(self):
        concurrency = APIUserResource.CONCURRENCY_LIMIT
        queue = DeferredQueue()
        url = '%s/%s/messages.json' % (self.url, self.api_username)
        max_receivers = [
            self.client.stream(
                TransportUserMessage, queue.put, queue.put, url,
                Headers(self.auth_headers))
            for _ in range(concurrency)]

        for i in range(concurrency):
            msg = self.mkmsg_in(content='in %s' % (i,), message_id='%s' % (i,))
            yield self.dispatch(msg)
            received = yield queue.get()
            self.assertEqual(msg['message_id'], received['message_id'])

        maxed_out_resp = yield http_request_full(url, method='GET',
                                                 headers=self.auth_headers)

        self.assertEqual(maxed_out_resp.code, 403)
        self.assertTrue('Too many concurrent connections'
                        in maxed_out_resp.delivered_body)

        [r.disconnect() for r in max_receivers]

    @inlineCallbacks
    def test_backlog_on_connect(self):
        for i in range(10):
            msg = self.mkmsg_in(content='in %s' % (i,), message_id=str(i))
            yield self.dispatch(msg)

        queue = DeferredQueue()
        url = '%s/%s/messages.json' % (self.url, self.api_username)
        receiver = self.client.stream(TransportUserMessage, queue.put,
            queue.put, url, Headers(self.auth_headers))

        for i in range(10):
            received = yield queue.get()
            self.assertEqual(received['message_id'], str(i))

        receiver.disconnect()

    @inlineCallbacks
    def test_health_response(self):
        health_url = 'http://%s:%s%s' % (
            self.addr.host, self.addr.port, self.config['health_path'])

        response = yield http_request_full(health_url, method='GET')
        self.assertEqual(response.delivered_body, '0')

        msg = self.mkmsg_in(content='in 1', message_id='1')
        yield self.dispatch(msg)

        queue = DeferredQueue()
        stream_url = '%s/%s/messages.json' % (self.url, self.api_username)
        stream_receiver = self.client.stream(
            TransportUserMessage, queue.put, queue.put, stream_url,
            Headers(self.auth_headers))

        yield queue.get()

        response = yield http_request_full(health_url, method='GET')
        self.assertEqual(response.delivered_body, '1')

        stream_receiver.disconnect()

        response = yield http_request_full(health_url, method='GET')
        self.assertEqual(response.delivered_body, '0')

        self.assertEqual(self.app.client_manager.clients, {
            'sphex.stream.message.%s' % (self.api_username,): []
        })

    @inlineCallbacks
    def test_post_inbound_message(self):
        # Set the URL so stuff is HTTP Posted instead of streamed.
        self.app.config['push_message_url'] = self.mock_push_server.url

        msg = self.mkmsg_in(content='in 1', message_id='1')
        msg_d = self.dispatch(msg)

        req = yield self.push_calls.get()
        posted_json_data = req.content.read()
        req.finish()
        yield msg_d

        posted_msg = TransportUserMessage.from_json(posted_json_data)
        self.assertEqual(posted_msg['message_id'], msg['message_id'])

    @inlineCallbacks
    def test_post_inbound_event(self):
        # Set the URL so stuff is HTTP Posted instead of streamed.
        self.app.config['push_event_url'] = self.mock_push_server.url

        ack1 = self.mkmsg_ack(user_message_id='message_id_1')
        event_d = self.dispatch_event(ack1)

        req = yield self.push_calls.get()
        posted_json_data = req.content.read()
        req.finish()
        yield event_d

        self.assertEqual(TransportEvent.from_json(posted_json_data), ack1)

    @inlineCallbacks
    def test_bad_urls(self):
        def assert_not_found(url, headers={}):
            d = http_request_full(self.url, method='GET', headers=headers)
            d.addCallback(lambda r: self.assertEqual(r.code, http.NOT_FOUND))
            return d

        yield assert_not_found(self.url)
        yield assert_not_found(self.url + '/')
        yield assert_not_found('%s/%s' % (self.url, self.api_username),
                               headers=self.auth_headers)
        yield assert_not_found('%s/%s/' % (self.url, self.api_username),
                               headers=self.auth_headers)
        yield assert_not_found('%s/%s/foo' % (self.url, self.api_username),
                               headers=self.auth_headers)
