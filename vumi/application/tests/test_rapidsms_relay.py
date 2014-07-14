"""Tests for vumi.application.rapidsms_relay."""

import json

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.tests.utils import LogCatcher, MockHttpServer
from vumi.application.rapidsms_relay import RapidSMSRelay, BadRequestError
from vumi.utils import http_request_full, basic_auth_string, to_kwargs
from vumi.message import TransportUserMessage, from_json
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import VumiTestCase


class TestRapidSMSRelay(VumiTestCase):

    def setUp(self):
        self.app_helper = self.add_helper(ApplicationHelper(RapidSMSRelay))

    @inlineCallbacks
    def setup_resource(self, callback=None, auth=None, config=None):
        if callback is None:
            callback = lambda r: self.fail("No RapidSMS requests expected.")
        self.mock_server = MockHttpServer(callback)
        self.add_cleanup(self.mock_server.stop)
        yield self.mock_server.start()
        url = '%s%s' % (self.mock_server.url, '/test/resource/path')
        self.app = yield self.setup_app(url, auth=auth, config=config)

    def setup_app(self, url, auth=None, config=None):
        vumi_username, vumi_password = auth if auth else (None, None)
        app_config = {
            'rapidsms_url': url,
            'web_path': '/send/',
            'web_port': '0',
            'rapidsms_username': 'username',
            'rapidsms_password': 'password',
            'vumi_username': vumi_username,
            'vumi_password': vumi_password,
            'allowed_endpoints': ['default', '10010', '10020'],
        }
        if config:
            app_config.update(config)
        return self.app_helper.get_application(app_config)

    def get_response_msgs(self, response):
        payloads = from_json(response.delivered_body)
        return [TransportUserMessage(
                _process_fields=False, **to_kwargs(payload))
                for payload in payloads]

    @inlineCallbacks
    def test_rapidsms_relay_success(self):
        def cb(request):
            msg = TransportUserMessage.from_json(request.content.read())
            self.assertEqual(msg['content'], 'hello world')
            self.assertEqual(msg['from_addr'], '+41791234567')
            return 'OK'

        yield self.setup_resource(cb)
        yield self.app_helper.make_dispatch_inbound("hello world")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_rapidsms_relay_unicode(self):
        def cb(request):
            msg = TransportUserMessage.from_json(request.content.read())
            self.assertEqual(msg['content'], u'h\xc6llo')
            return 'OK'

        yield self.setup_resource(cb)
        yield self.app_helper.make_dispatch_inbound(u'h\xc6llo')
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_rapidsms_relay_with_basic_auth(self):
        def cb(request):
            self.assertEqual(request.getUser(), 'username')
            self.assertEqual(request.getPassword(), 'password')
            msg = TransportUserMessage.from_json(request.content.read())
            self.assertEqual(msg['message_id'], 'abc')
            self.assertEqual(msg['content'], 'hello world')
            self.assertEqual(msg['from_addr'], '+41791234567')
            return 'OK'

        yield self.setup_resource(cb)
        yield self.app_helper.make_dispatch_inbound(
            "hello world", message_id="abc")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_rapidsms_relay_with_bad_basic_auth(self):
        def cb(request):
            request.setResponseCode(http.UNAUTHORIZED)
            return 'Not Authorized'

        yield self.setup_resource(cb)
        yield self.app_helper.make_dispatch_inbound("hi")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_rapidsms_relay_logs_events(self):
        yield self.setup_resource()
        with LogCatcher() as lc:
            yield self.app_helper.make_dispatch_delivery_report(
                self.app_helper.make_outbound("foo", message_id="abc"))
            yield self.app_helper.make_dispatch_ack(
                self.app_helper.make_outbound("foo", message_id="1"))
            self.assertEqual(lc.messages(), [
                "Delivery report received for message u'abc',"
                " status u'delivered'",
                "Acknowledgement received for message u'1'",
            ])
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    @inlineCallbacks
    def test_rapidsms_relay_with_unicode_rapidsms_http_method(self):
        def cb(request):
            msg = TransportUserMessage.from_json(request.content.read())
            self.assertEqual(msg['content'], 'hello world')
            self.assertEqual(msg['from_addr'], '+41791234567')
            return 'OK'

        yield self.setup_resource(cb, config={"rapidsms_http_method": u"POST"})
        yield self.app_helper.make_dispatch_inbound("hello world")
        self.assertEqual([], self.app_helper.get_dispatched_outbound())

    def _call_relay(self, data, auth=None):
        data = json.dumps(data)
        host = self.app.web_resource.getHost()
        send_url = "http://127.0.0.1:%d/send" % (host.port,)
        headers = {}
        if auth is not None:
            headers['Authorization'] = basic_auth_string(*auth)
        return http_request_full(send_url, data, headers=headers)

    def _check_messages(self, response, expecteds):
        response_msgs = self.get_response_msgs(response)
        msgs = self.app_helper.get_dispatched_outbound()
        for rmsg, msg, expected in zip(response_msgs, msgs, expecteds):
            self.assertEqual(msg, rmsg)
            for k, v in expected.items():
                self.assertEqual(msg[k], v)
        self.assertEqual(len(msgs), len(expecteds))
        self.assertEqual(len(response_msgs), len(expecteds))

    @inlineCallbacks
    def test_rapidsms_relay_outbound(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': 'foo',
        })
        self.assertEqual(response.headers.getRawHeaders('content-type'),
                         ['application/json; charset=utf-8'])
        self._check_messages(response, [
            {'to_addr': '+123456', 'content': u'foo'}])

    @inlineCallbacks
    def test_rapidsms_relay_outbound_unicode(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': u'f\xc6r',
        })
        self._check_messages(response, [
            {'to_addr': '+123456', 'content': u'f\xc6r'}])

    @inlineCallbacks
    def test_rapidsms_relay_multiple_outbound(self):
        yield self.setup_resource()
        addresses = ['+123456', '+678901']
        response = yield self._call_relay({
            'to_addr': addresses,
            'content': 'foo',
        })
        self._check_messages(response, [
            {'to_addr': addr, 'content': u'foo'}
            for addr in addresses])

    @inlineCallbacks
    def test_rapidsms_relay_reply(self):
        msg_id, to_addr = 'abc', '+1234'
        yield self.setup_resource(lambda r: 'OK')
        yield self.app_helper.make_dispatch_inbound(
            "foo", message_id=msg_id, from_addr=to_addr)
        response = yield self._call_relay({
            'to_addr': [to_addr],
            'content': 'foo',
            'in_reply_to': msg_id,
        })
        self._check_messages(response, [
            {'to_addr': to_addr, 'content': u'foo', 'in_reply_to': msg_id}])

    @inlineCallbacks
    def test_rapidsms_relay_reply_unknown_msg(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': 'foo',
            'in_reply_to': 'unknown_message_id',
        })
        self.assertEqual(response.code, 400)
        self.assertEqual(response.delivered_body,
                         "Original message u'unknown_message_id' not found.")
        [err] = self.flushLoggedErrors(BadRequestError)

    @inlineCallbacks
    def test_rapidsms_relay_outbound_authenticated(self):
        auth = ("username", "good-password")
        yield self.setup_resource(callback=None, auth=auth)
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': u'f\xc6r',
        }, auth=auth)
        self._check_messages(response, [
            {'to_addr': '+123456', 'content': u'f\xc6r'}])

    @inlineCallbacks
    def test_rapidsms_relay_outbound_failed_authenticated(self):
        bad_auth = ("username", "bad-password")
        good_auth = ("username", "good-password")
        yield self.setup_resource(callback=None, auth=good_auth)
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': u'f\xc6r',
        }, auth=bad_auth)
        self.assertEqual(response.code, 401)
        self.assertEqual(response.delivered_body, "Unauthorized")

    @inlineCallbacks
    def test_rapidsms_relay_outbound_on_specific_endpoint(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': u'foo',
            'endpoint': '10010',
        })
        self._check_messages(response, [
            {'to_addr': '+123456', 'content': u'foo'}])
        [msg] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(msg['routing_metadata'], {
            'endpoint_name': '10010',
        })

    @inlineCallbacks
    def test_rapidsms_relay_outbound_on_default_endpoint(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': u'foo',
        })
        self._check_messages(response, [
            {'to_addr': '+123456', 'content': u'foo'}])
        [msg] = self.app_helper.get_dispatched_outbound()
        self.assertEqual(msg['routing_metadata'], {
            'endpoint_name': 'default',
        })

    @inlineCallbacks
    def test_rapidsms_relay_outbound_on_invalid_endpoint(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': ['+123456'],
            'content': u'foo',
            'endpoint': u'bar',
        })
        self.assertEqual([], self.app_helper.get_dispatched_outbound())
        self.assertEqual(response.code, 400)
        self.assertEqual(response.delivered_body,
                         "Endpoint u'bar' not defined in list of allowed"
                         " endpoints ['default', '10010', '10020']")
        [err] = self.flushLoggedErrors(BadRequestError)

    @inlineCallbacks
    def test_rapidsms_relay_outbound_on_invalid_to_addr(self):
        yield self.setup_resource()
        response = yield self._call_relay({
            'to_addr': '+123456',
            'content': u'foo',
            'endpoint': u'bar',
        })
        self.assertEqual([], self.app_helper.get_dispatched_outbound())
        self.assertEqual(response.code, 400)
        self.assertEqual(response.delivered_body,
                         "Supplied `to_addr` (u'+123456') was not a list.")
        [err] = self.flushLoggedErrors(BadRequestError)
