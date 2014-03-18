import hashlib
from urllib import urlencode
from datetime import datetime

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase
from twisted.web.server import NOT_DONE_YET

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.wechat import WeChatTransport
from vumi.transports.wechat.parser import WeChatParser
from vumi.transports.wechat import message_types
from vumi.utils import http_request_full


def request(transport, method, path='', params={}, data=None):
    addr = transport.server.getHost()
    if params:
        path += '?%s' % (urlencode(params),)
    url = 'http://%s:%s%s%s' % (
        addr.host,
        addr.port,
        transport.get_static_config().web_path,
        path)
    return http_request_full(url, method=method, data=data)


class WeChatTestCase(VumiTestCase):

    transport_class = WeChatTransport

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.mock_server = MockHttpServer(self.handle_inbound_request)
        self.add_cleanup(self.mock_server.stop)
        self.clock = Clock()

        yield self.mock_server.start()

    def get_transport(self, **config):
        defaults = {
            'api_url': self.mock_server.url,
            'auth_token': 'token',
            'twisted_endpoint': 'tcp:0'
        }
        defaults.update(config)
        return self.tx_helper.get_transport(defaults)

    def handle_inbound_request(self, request):
        reactor.callLater(0, request.finish)
        return NOT_DONE_YET

    @inlineCallbacks
    def test_auth_success(self):
        transport = yield self.get_transport()

        nonce = '1234'
        timestamp = '2014-01-01T00:00:00'
        good_signature = hashlib.sha1(
            ''.join(sorted([timestamp, nonce, 'token']))).hexdigest()

        resp = yield request(
            transport, "GET", params={
                'signature': good_signature,
                'timestamp': timestamp,
                'nonce': nonce,
                'echostr': 'success'
            })
        self.assertEqual(resp.delivered_body, 'success')

    @inlineCallbacks
    def test_auth_fail(self):
        transport = yield self.get_transport()

        nonce = '1234'
        timestamp = '2014-01-01T00:00:00'
        bad_signature = hashlib.sha1(
            ''.join(sorted([timestamp, nonce, 'bad_token']))).hexdigest()

        resp = yield request(
            transport, "GET", params={
                'signature': bad_signature,
                'timestamp': timestamp,
                'nonce': nonce,
                'echostr': 'success'
            })
        self.assertNotEqual(resp.delivered_body, 'success')

    @inlineCallbacks
    def test_inbound_text_message(self):
        transport = yield self.get_transport()

        resp_d = request(
            transport, 'POST', data="""
            <xml>
            <ToUserName><![CDATA[toUser]]></ToUserName>
            <FromUserName><![CDATA[fromUser]]></FromUserName>
            <CreateTime>1348831860</CreateTime>
            <MsgType><![CDATA[text]]></MsgType>
            <Content><![CDATA[this is a test]]></Content>
            <MsgId>1234567890123456</MsgId>
            </xml>
            """.strip())

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(
            msg, 'foo')

        resp = yield resp_d
        print resp.delivered_body


class WeChatParserTestCase(TestCase):

    def test_parse_text_message(self):
        msg = WeChatParser.parse(
            """
            <xml>
            <ToUserName><![CDATA[toUser]]></ToUserName>
            <FromUserName><![CDATA[fromUser]]></FromUserName>
            <CreateTime>1348831860</CreateTime>
            <MsgType><![CDATA[text]]></MsgType>
            <Content><![CDATA[this is a test]]></Content>
            <MsgId>1234567890123456</MsgId>
            </xml>
            """)

        self.assertEqual(msg.ToUserName, 'toUser')
        self.assertEqual(msg.FromUserName, 'fromUser')
        self.assertEqual(msg.CreateTime, datetime.fromtimestamp(1348831860))
        self.assertEqual(msg.MsgId, '1234567890123456')
        self.assertTrue(isinstance(msg, message_types.TextMessage))

    def test_parse_both_ways(self):
        # this is what we generate
        msg1 = message_types.TextMessage({
            'ToUserName': 'toUser',
            'FromUserName': 'fromUser',
            'CreateTime': datetime.fromtimestamp(1348831860),
            'Content': 'this is a test',
            'MsgId': '1234567890123456',
        })
        # this is what wechat gives us
        msg2 = WeChatParser.parse(
            """
            <xml>
            <ToUserName><![CDATA[toUser]]></ToUserName>
            <FromUserName><![CDATA[fromUser]]></FromUserName>
            <CreateTime>1348831860</CreateTime>
            <MsgType><![CDATA[text]]></MsgType>
            <Content><![CDATA[this is a test]]></Content>
            <MsgId>1234567890123456</MsgId>
            </xml>
            """)
        self.assertEqual(msg1, msg2)
