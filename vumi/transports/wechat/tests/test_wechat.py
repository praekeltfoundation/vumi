import hashlib
import json
from urllib import urlencode
from datetime import datetime

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase
from twisted.web import http
from twisted.web.server import NOT_DONE_YET

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.wechat import WeChatTransport
from vumi.transports.wechat.parser import WeChatParser
from vumi.transports.wechat import message_types
from vumi.utils import http_request_full
from vumi.message import TransportUserMessage


def request(transport, method, path='', params={}, data=None):
    addr = transport.server.getHost()
    token = transport.get_static_config().auth_token

    nonce = '1234'
    timestamp = '2014-01-01T00:00:00'
    good_signature = hashlib.sha1(
        ''.join(sorted([timestamp, nonce, token]))).hexdigest()

    default_params = {
        'signature': good_signature,
        'timestamp': timestamp,
        'nonce': nonce,
    }

    default_params.update(params)
    path += '?%s' % (urlencode(default_params),)
    url = 'http://%s:%s%s%s' % (
        addr.host,
        addr.port,
        transport.get_static_config().web_path,
        path)
    return http_request_full(url, method=method, data=data)


class WeChatTestCase(VumiTestCase):

    transport_class = WeChatTransport

    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(self.transport_class))
        self.request_queue = DeferredQueue()
        self.mock_server = MockHttpServer(self.handle_api_request)
        self.add_cleanup(self.mock_server.stop)
        self.clock = Clock()
        return self.mock_server.start()

    def handle_api_request(self, request):
        self.request_queue.put(request)
        return NOT_DONE_YET

    def get_transport(self, **config):
        defaults = {
            'api_url': self.mock_server.url,
            'auth_token': 'token',
            'twisted_endpoint': 'tcp:0'
        }
        defaults.update(config)
        return self.tx_helper.get_transport(defaults)

    @inlineCallbacks
    def test_auth_success(self):
        transport = yield self.get_transport()
        resp = yield request(
            transport, "GET", params={
                'echostr': 'success'
            })
        self.assertEqual(resp.delivered_body, 'success')

    @inlineCallbacks
    def test_auth_fail(self):
        transport = yield self.get_transport()
        resp = yield request(
            transport, "GET", params={
                'signature': 'foo',
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
        reply_msg = yield self.tx_helper.make_dispatch_reply(
            msg, 'foo')

        resp = yield resp_d
        reply = WeChatParser.parse(resp.delivered_body)
        self.assertEqual(reply.ToUserName, 'fromUser')
        self.assertEqual(reply.FromUserName, 'toUser')
        self.assertTrue(reply.CreateTime > datetime.fromtimestamp(1348831860))
        self.assertTrue(isinstance(reply, message_types.TextMessage))

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], reply_msg['message_id'])
        self.assertEqual(ack['sent_message_id'], reply_msg['message_id'])

    @inlineCallbacks
    def test_inbound_event_subscribe_message(self):
        transport = yield self.get_transport()

        resp_d = request(
            transport, 'POST', data="""
                <xml>
                    <ToUserName>
                        <![CDATA[toUser]]>
                    </ToUserName>
                    <FromUserName>
                        <![CDATA[fromUser]]>
                    </FromUserName>
                    <CreateTime>1395130515</CreateTime>
                    <MsgType>
                        <![CDATA[event]]>
                    </MsgType>
                    <Event>
                        <![CDATA[subscribe]]>
                    </Event>
                    <EventKey>
                        <![CDATA[]]>
                    </EventKey>
                </xml>
                """)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(
            msg['session_event'], TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'wechat': {
                'Event': 'subscribe',
                'EventKey': None,
                'FromUserName': 'fromUser',
                'MsgType': 'event',
                'ToUserName': 'toUser'
            }
        })

        self.tx_helper.make_dispatch_reply(
            msg, 'foo')

        resp = yield resp_d
        reply = WeChatParser.parse(resp.delivered_body)
        self.assertEqual(reply.ToUserName, 'fromUser')
        self.assertEqual(reply.FromUserName, 'toUser')
        self.assertTrue(reply.CreateTime > datetime.fromtimestamp(1348831860))
        self.assertTrue(isinstance(reply, message_types.TextMessage))

    @inlineCallbacks
    def test_unsupported_message_type(self):
        transport = yield self.get_transport()

        response = yield request(
            transport, 'POST', data="""
            <xml>
            <ToUserName><![CDATA[toUser]]></ToUserName>
            <FromUserName><![CDATA[fromUser]]></FromUserName>
            <CreateTime>1348831860</CreateTime>
            <MsgType><![CDATA[THIS_IS_UNSUPPORTED]]></MsgType>
            <Content><![CDATA[this is a test]]></Content>
            <MsgId>1234567890123456</MsgId>
            </xml>
            """.strip())

        self.assertEqual(
            response.code, http.BAD_REQUEST)
        self.assertEqual(
            response.delivered_body,
            ("Unparseable WeChat Message Element "
             "u'MsgType': u'THIS_IS_UNSUPPORTED'"))
        self.assertEqual(
            [],
            self.tx_helper.get_dispatched_inbound())

    @inlineCallbacks
    def test_push_invalid_message(self):
        transport = yield self.get_transport()
        msg = yield self.tx_helper.make_dispatch_outbound('foo')
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['nack_reason'], 'Missing MsgType')

    def dispatch_push_message(self, content, wechat_md, **kwargs):
        helper_metadata = kwargs.get('helper_metadata', {})
        wechat_metadata = helper_metadata.setdefault('wechat', {})
        wechat_metadata.update(wechat_md)
        return self.tx_helper.make_dispatch_outbound(
            content, helper_metadata=helper_metadata, **kwargs)

    @inlineCallbacks
    def test_push_unsupported_message(self):
        transport = yield self.get_transport()
        msg = yield self.dispatch_push_message('foo', {
            'MsgType': 'foo'
        })
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(
            nack['nack_reason'], "Unsupported MsgType: u'foo'")

    @inlineCallbacks
    def test_ack_push_text_message(self):
        transport = yield self.get_transport()

        msg_d = self.dispatch_push_message('foo', {
            'MsgType': 'text',
        }, to_addr='toaddr')

        request = yield self.request_queue.get()
        self.assertEqual(request.path, '/message/custom/send')
        self.assertEqual(request.args, {
            'access_token': ['foo']
        })
        self.assertEqual(json.load(request.content), {
            'touser': 'toaddr',
            'msgtype': 'text',
            'text': {
                'content': 'foo'
            }
        })
        request.finish()

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        msg = yield msg_d
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_nack_push_text_message(self):
        transport = yield self.get_transport()

        self.dispatch_push_message('foo', {
            'MsgType': 'text'
        })

        # fail the API request
        request = yield self.request_queue.get()
        request.setResponseCode(http.BAD_REQUEST)
        request.finish()

        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['nack_reason'], 'Received status code: 400')

    @inlineCallbacks
    def test_ack_push_news_message(self):
        transport = yield self.get_transport()

        # news is a collection or URLs apparently
        msg_d = self.dispatch_push_message(
            'foo', {
                'MsgType': 'news',
                'news': {
                    'articles': [
                        {
                            'title': 'title',
                            'description': 'description',
                            'url': 'http://url',
                            'picurl': 'http://picurl',
                        }
                    ]
                }
            }, to_addr='toaddr')

        request = yield self.request_queue.get()
        self.assertEqual(request.path, '/message/custom/send')
        self.assertEqual(request.args, {
            'access_token': ['foo']
        })
        self.assertEqual(json.load(request.content), {
            'touser': 'toaddr',
            'msgtype': 'news',
            'news': {
                'articles': [
                    {
                        'title': 'title',
                        'description': 'description',
                        'url': 'http://url',
                        'picurl': 'http://picurl',
                    }
                ]
            }
        })

        request.finish()

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        msg = yield msg_d
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], msg['message_id'])


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

    def test_parse_event_message(self):
        event = WeChatParser.parse(
            """
            <xml>
                <ToUserName>
                    <![CDATA[toUser]]>
                </ToUserName>
                <FromUserName>
                    <![CDATA[fromUser]]>
                </FromUserName>
                <CreateTime>1395130515</CreateTime>
                <MsgType>
                    <![CDATA[event]]>
                </MsgType>
                <Event>
                    <![CDATA[subscribe]]>
                </Event>
                <EventKey>
                    <![CDATA[]]>
                </EventKey>
            </xml>
            """)
        self.assertEqual(event.ToUserName, 'toUser')
        self.assertEqual(event.FromUserName, 'fromUser')
        self.assertEqual(event.CreateTime, datetime.fromtimestamp(1395130515))
        self.assertEqual(event.Event, 'subscribe')
        self.assertEqual(event.EventKey, None)
        self.assertTrue(isinstance(event, message_types.EventMessage))
