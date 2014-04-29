import hashlib
import json
import yaml
from urllib import urlencode

from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, returnValue, gatherResults)
from twisted.internet import task, reactor
from twisted.web import http
from twisted.web.server import NOT_DONE_YET
from twisted.trial.unittest import SkipTest

from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer, LogCatcher
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.wechat import WeChatTransport
from vumi.transports.wechat.errors import WeChatApiException
from vumi.transports.wechat.message_types import (
    WeChatXMLParser, TextMessage)
from vumi.utils import http_request_full
from vumi.message import TransportUserMessage
from vumi.persist.fake_redis import FakeRedis


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

    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(WeChatTransport))
        self.request_queue = DeferredQueue()
        self.mock_server = MockHttpServer(self.handle_api_request)
        self.add_cleanup(self.mock_server.stop)
        return self.mock_server.start()

    def handle_api_request(self, request):
        self.request_queue.put(request)
        return NOT_DONE_YET

    def get_transport(self, **config):
        defaults = {
            'api_url': self.mock_server.url,
            'auth_token': 'token',
            'twisted_endpoint': 'tcp:0',
            'wechat_appid': 'appid',
            'wechat_secret': 'secret',
            'embed_user_profile': False,
        }
        defaults.update(config)
        return self.tx_helper.get_transport(defaults)

    @inlineCallbacks
    def get_transport_with_access_token(self, access_token, **config):
        transport = yield self.get_transport(**config)
        yield transport.redis.set(WeChatTransport.ACCESS_TOKEN_KEY,
                                  access_token)
        returnValue(transport)


class TestWeChatInboundMessaging(WeChatTestCase):

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
        transport = yield self.get_transport_with_access_token('foo')
        resp = yield request(
            transport, "GET", params={
                'signature': 'foo',
                'echostr': 'success'
            })
        self.assertNotEqual(resp.delivered_body, 'success')

    @inlineCallbacks
    def test_inbound_text_message(self):
        transport = yield self.get_transport_with_access_token('foo')

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
        reply = WeChatXMLParser.parse(resp.delivered_body)
        self.assertEqual(reply.to_user_name, 'fromUser')
        self.assertEqual(reply.from_user_name, 'toUser')
        self.assertTrue(int(reply.create_time) > 1348831860)
        self.assertTrue(isinstance(reply, TextMessage))

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], reply_msg['message_id'])
        self.assertEqual(ack['sent_message_id'], reply_msg['message_id'])

    @inlineCallbacks
    def test_inbound_event_subscribe_message(self):
        transport = yield self.get_transport_with_access_token('foo')

        resp = yield request(
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
                    <EventKey><![CDATA[]]></EventKey>
                </xml>
                """)
        self.assertEqual(resp.code, http.OK)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(
            msg['session_event'], TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'wechat': {
                'Event': 'subscribe',
                'EventKey': '',
                'FromUserName': 'fromUser',
                'MsgType': 'event',
                'ToUserName': 'toUser'
            }
        })

    @inlineCallbacks
    def test_inbound_menu_event_click_message(self):
        transport = yield self.get_transport_with_access_token('foo')

        resp = yield request(
            transport, 'POST', data="""
                <xml>
                <ToUserName><![CDATA[toUser]]></ToUserName>
                <FromUserName><![CDATA[fromUser]]></FromUserName>
                <CreateTime>123456789</CreateTime>
                <MsgType><![CDATA[event]]></MsgType>
                <Event><![CDATA[CLICK]]></Event>
                <EventKey><![CDATA[EVENTKEY]]></EventKey>
                </xml>
                """.strip())
        self.assertEqual(resp.code, http.OK)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(
            msg['session_event'], TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'wechat': {
                'Event': 'CLICK',
                'EventKey': 'EVENTKEY',
                'FromUserName': 'fromUser',
                'MsgType': 'event',
                'ToUserName': 'toUser'
            }
        })

        self.assertEqual(msg['to_addr'], 'toUser@EVENTKEY')

    @inlineCallbacks
    def test_inbound_menu_event_view_message(self):
        transport = yield self.get_transport_with_access_token('foo')

        with LogCatcher() as lc:
            resp = yield request(
                transport, 'POST', data="""
                    <xml>
                    <ToUserName><![CDATA[toUser]]></ToUserName>
                    <FromUserName><![CDATA[fromUser]]></FromUserName>
                    <CreateTime>123456789</CreateTime>
                    <MsgType><![CDATA[event]]></MsgType>
                    <Event><![CDATA[VIEW]]></Event>
                    <EventKey><![CDATA[http://www.gotvafrica.com/mobi/home.aspx]]></EventKey>
                    </xml>
                    """.strip())
            self.assertEqual(resp.code, http.OK)
            [] = self.tx_helper.get_dispatched_inbound()
            msg = lc.messages()[0]
            self.assertEqual(
                msg,
                'fromUser clicked on http://www.gotvafrica.com/mobi/home.aspx')

    @inlineCallbacks
    def test_unsupported_message_type(self):
        transport = yield self.get_transport_with_access_token('foo')

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
            "Unsupported MsgType: THIS_IS_UNSUPPORTED")
        self.assertEqual(
            [],
            self.tx_helper.get_dispatched_inbound())


class TestWeChatOutboundMessaging(WeChatTestCase):

    def dispatch_push_message(self, content, wechat_md, **kwargs):
        helper_metadata = kwargs.get('helper_metadata', {})
        wechat_metadata = helper_metadata.setdefault('wechat', {})
        wechat_metadata.update(wechat_md)
        return self.tx_helper.make_dispatch_outbound(
            content, helper_metadata=helper_metadata, **kwargs)

    @inlineCallbacks
    def test_ack_push_text_message(self):
        yield self.get_transport_with_access_token('foo')

        msg_d = self.dispatch_push_message('foo', {}, to_addr='toaddr')

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
        yield self.get_transport_with_access_token('foo')
        msg_d = self.dispatch_push_message('foo', {})

        # fail the API request
        request = yield self.request_queue.get()
        request.setResponseCode(http.BAD_REQUEST)
        request.finish()

        msg = yield msg_d
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(
            nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['nack_reason'], 'Received status code: 400')

    @inlineCallbacks
    def test_ack_push_inferred_news_message(self):
        yield self.get_transport_with_access_token('foo')
        # news is a collection or URLs apparently
        content = ('This is an awesome link for you! http://www.wechat.com/ '
                   'Go visit it.')
        msg_d = self.dispatch_push_message(
            content, {}, to_addr='toaddr')

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
                        'title': 'This is an awesome link for you! ',
                        'url': 'http://www.wechat.com/',
                        'description': content,
                    }
                ]
            }
        })

        request.finish()

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        msg = yield msg_d
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], msg['message_id'])


class TestWeChatAccessToken(WeChatTestCase):

    @inlineCallbacks
    def test_request_new_access_token(self):
        transport = yield self.get_transport()
        config = transport.get_static_config()

        d = transport.request_new_access_token()

        req = yield self.request_queue.get()
        self.assertEqual(req.path, '/token')
        self.assertEqual(req.args, {
            'grant_type': ['client_credential'],
            'appid': [config.wechat_appid],
            'secret': [config.wechat_secret],
        })
        req.write(json.dumps({
            'access_token': 'the_access_token',
            'expires_in': 7200
        }))
        req.finish()

        access_token = yield d
        self.assertEqual(access_token, 'the_access_token')
        cached_token = yield transport.redis.get(
            WeChatTransport.ACCESS_TOKEN_KEY)
        self.assertEqual(cached_token, 'the_access_token')
        expiry = yield transport.redis.ttl(WeChatTransport.ACCESS_TOKEN_KEY)
        self.assertTrue(int(7200 * 0.8) < expiry <= int(7200 * 0.9))

    @inlineCallbacks
    def test_get_cached_access_token(self):
        transport = yield self.get_transport()
        yield transport.redis.set(WeChatTransport.ACCESS_TOKEN_KEY, 'foo')
        access_token = yield transport.get_access_token()
        self.assertEqual(access_token, 'foo')
        # Empty request queue means no WeChat API calls were made
        self.assertEqual(self.request_queue.size, None)


class TestWeChatAddrMasking(WeChatTestCase):

    @inlineCallbacks
    def test_default_mask(self):
        transport = yield self.get_transport_with_access_token('foo')

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
        yield self.tx_helper.make_dispatch_reply(msg, 'foo')

        self.assertEqual(
            (yield transport.get_addr_mask('fromUser')),
            transport.DEFAULT_MASK)
        self.assertEqual(msg['to_addr'], 'toUser@default')
        yield resp_d

    @inlineCallbacks
    def test_mask_switching_on_event_key(self):
        transport = yield self.get_transport_with_access_token('foo')

        resp = yield request(
            transport, 'POST', data="""
                <xml>
                <ToUserName><![CDATA[toUser]]></ToUserName>
                <FromUserName><![CDATA[fromUser]]></FromUserName>
                <CreateTime>123456789</CreateTime>
                <MsgType><![CDATA[event]]></MsgType>
                <Event><![CDATA[CLICK]]></Event>
                <EventKey><![CDATA[EVENTKEY]]></EventKey>
                </xml>
                """.strip())
        self.assertEqual(resp.code, http.OK)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(
            msg['session_event'], TransportUserMessage.SESSION_NEW)

        self.assertEqual(
            (yield transport.get_addr_mask('fromUser')), 'EVENTKEY')
        self.assertEqual(msg['to_addr'], 'toUser@EVENTKEY')

    @inlineCallbacks
    def test_mask_caching_on_text_message(self):
        transport = yield self.get_transport_with_access_token('foo')
        yield transport.cache_addr_mask('fromUser', 'foo')

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
        yield self.tx_helper.make_dispatch_reply(msg, 'foo')

        self.assertEqual(msg['to_addr'], 'toUser@foo')
        yield resp_d

    @inlineCallbacks
    def test_mask_clearing_on_session_end(self):
        transport = yield self.get_transport_with_access_token('foo')
        yield transport.cache_addr_mask('fromUser', 'foo')

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
        yield self.tx_helper.make_dispatch_reply(
            msg, 'foo', session_event=TransportUserMessage.SESSION_CLOSE)

        self.assertEqual(msg['to_addr'], 'toUser@foo')
        self.assertEqual(
            (yield transport.get_addr_mask('fromUser')),
            transport.DEFAULT_MASK)
        yield resp_d

    @inlineCallbacks
    def test_inbound_event_unsubscribe_message(self):
        transport = yield self.get_transport_with_access_token('foo')
        yield transport.cache_addr_mask('fromUser', 'foo')

        resp = yield request(
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
                        <![CDATA[unsubscribe]]>
                    </Event>
                    <EventKey><![CDATA[]]></EventKey>
                </xml>
                """)
        self.assertEqual(resp.code, http.OK)
        self.assertEqual([], self.tx_helper.get_dispatched_inbound())
        self.assertEqual(
            (yield transport.get_addr_mask('fromUser')),
            transport.DEFAULT_MASK)


class TestWeChatMenuCreation(WeChatTestCase):

    MENU_TEMPLATE = """
    button:
      - name: Daily Song
        type: click
        key: V1001_TODAY_MUSIC

      - name: ' Artist Profile'
        type: click
        key: V1001_TODAY_SINGER

      - name: Menu
        sub_button:
          - name: Search
            type: view
            url: 'http://www.soso.com/'
          - name: Video
            type: view
            url: 'http://v.qq.com/'
          - name: Like us
            type: click
            key: V1001_GOOD
    """
    MENU = yaml.safe_load(MENU_TEMPLATE)

    @inlineCallbacks
    def test_create_new_menu_success(self):
        transport = yield self.get_transport_with_access_token('foo')

        d = transport.create_wechat_menu('foo', self.MENU)
        req = yield self.request_queue.get()
        self.assertEqual(req.path, '/menu/create')
        self.assertEqual(req.args, {
            'access_token': ['foo'],
        })

        self.assertEqual(json.load(req.content), self.MENU)
        req.write(json.dumps({'errcode': 0, 'errmsg': 'ok'}))
        req.finish()

        yield d

    @inlineCallbacks
    def test_create_new_menu_failure(self):
        transport = yield self.get_transport_with_access_token('foo')
        d = transport.create_wechat_menu('foo', self.MENU)

        req = yield self.request_queue.get()
        req.write(json.dumps({
            'errcode': 40018,
            'errmsg': 'invalid button name size',
        }))
        req.finish()

        exception = yield self.assertFailure(d, WeChatApiException)
        self.assertEqual(
            exception.message,
            ('Received errcode: 40018, errmsg: invalid button name '
             'size when creating WeChat Menu.'))


class TestWeChatInferMessage(WeChatTestCase):

    @inlineCallbacks
    def test_infer_news_message(self):
        transport = yield self.get_transport_with_access_token('foo')

        resp_d = request(
            transport, 'POST', data="""
            <xml>
            <ToUserName><![CDATA[toUser]]></ToUserName>
            <FromUserName><![CDATA[fromUser]]></FromUserName>
            <CreateTime>1348831860</CreateTime>
            <MsgType><![CDATA[text]]></MsgType>
            <Content><![CDATA[this is a test]]></Content>
            <MsgId>10234567890123456</MsgId>
            </xml>
            """.strip())

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(
            msg, ('To continue you need to accept the T&Cs available at '
                  'http://tandcurl.com/ . Have you read and do you accept '
                  'the terms and conditions?\n1. Yes\n2. No'))

        resp = yield resp_d
        self.assertTrue(
            '<Url>http://tandcurl.com/</Url>' in resp.delivered_body)
        self.assertTrue(
            '<Title>To continue you need to accept the T&amp;Cs available '
            'at </Title>'
            in resp.delivered_body)
        self.assertTrue(
            '<Description>To continue you need to accept the T&amp;Cs '
            'available at http://tandcurl.com/ . Have you read and do '
            'you accept the terms and conditions?\n1. Yes\n2. No'
            '</Description>'
            in resp.delivered_body)


class TestWeChatEmbedUserProfile(WeChatTestCase):

    @inlineCallbacks
    def test_embed_user_profile(self):
        # NOTE: From http://admin.wechat.com/wiki/index.php?title=User_Profile
        user_profile = {
            "subscribe": 1,
            "openid": "fromUser",
            "nickname": "Band",
            "sex": 1,
            "language": "zh_CN",
            "city": "Guangzhou",
            "province": "Guangdong",
            "country": "China",
            "headimgurl": (
                "http://wx.qlogo.cn/mmopen/g3MonUZtNHkdmzicIlibx6iaFqAc56v"
                "xLSUfpb6n5WKSYVY0ChQKkiaJSgQ1dZuTOgvLLrhJbERQQ4eMsv84eavH"
                "iaiceqxibJxCfHe/0"),
            "subscribe_time": 1382694957
        }

        transport = yield self.get_transport_with_access_token(
            'foo', embed_user_profile=True)
        resp_d = request(
            transport, 'POST', data="""
            <xml>
            <ToUserName><![CDATA[toUser]]></ToUserName>
            <FromUserName><![CDATA[fromUser]]></FromUserName>
            <CreateTime>1348831860</CreateTime>
            <MsgType><![CDATA[text]]></MsgType>
            <Content><![CDATA[this is a test]]></Content>
            <MsgId>10234567890123456</MsgId>
            </xml>
            """.strip())

        req = yield self.request_queue.get()
        self.assertEqual(req.args, {
            'access_token': ['foo'],
            'lang': ['en'],
            'openid': ['fromUser'],
        })

        req.write(json.dumps(user_profile))
        req.finish()

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(msg, 'Bye!')

        self.assertEqual(
            msg['transport_metadata']['wechat']['UserProfile'],
            user_profile)

        up_key = transport.user_profile_key('fromUser')
        cached_up = yield transport.redis.get(up_key)
        config = transport.get_static_config()
        self.assertEqual(json.loads(cached_up), user_profile)
        self.assertTrue(0
                        < (yield transport.redis.ttl(up_key))
                        <= config.embed_user_profile_lifetime)
        yield resp_d


class TestWeChatInsanity(WeChatTestCase):

    @inlineCallbacks
    def test_double_delivery_handling(self):
        transport = yield self.get_transport_with_access_token('foo')

        xml = """
        <xml>
        <ToUserName><![CDATA[toUser]]></ToUserName>
        <FromUserName><![CDATA[fromUser]]></FromUserName>
        <CreateTime>1348831860</CreateTime>
        <MsgType><![CDATA[text]]></MsgType>
        <Content><![CDATA[this is a test]]></Content>
        <MsgId>1234567890123456</MsgId>
        </xml>
        """.strip()

        resp1_d = request(transport, 'POST', data=xml)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        reply_msg = yield self.tx_helper.make_dispatch_reply(
            msg, 'foo')

        resp1 = yield resp1_d
        reply1 = WeChatXMLParser.parse(resp1.delivered_body)
        self.assertTrue(isinstance(reply1, TextMessage))

        # this one should bounce straight away
        resp2 = yield request(transport, 'POST', data=xml)
        self.assertEqual(resp2.code, http.OK)
        reply2 = WeChatXMLParser.parse(resp2.delivered_body)
        self.assertEqual(reply1.to_xml(), reply2.to_xml())
        # Nothing new was added
        self.assertEqual(1, len(self.tx_helper.get_dispatched_inbound()))

    @inlineCallbacks
    def test_close_double_delivery_handling(self):
        transport = yield self.get_transport_with_access_token('foo')

        xml = """
        <xml>
        <ToUserName><![CDATA[toUser]]></ToUserName>
        <FromUserName><![CDATA[fromUser]]></FromUserName>
        <CreateTime>1348831860</CreateTime>
        <MsgType><![CDATA[text]]></MsgType>
        <Content><![CDATA[%s]]></Content>
        <MsgId>1234567890123456</MsgId>
        </xml>
        """.strip()

        resp1_d = request(transport, 'POST', data=xml % ('first',))
        resp2_d = task.deferLater(reactor, 0.1, request, transport, 'POST',
                                  data=xml % ('second',))

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        # the second request should return first
        resp2 = yield resp2_d
        self.assertEqual(resp2.code, http.OK)
        self.assertEqual(resp2.delivered_body, '')

        reply_msg = yield self.tx_helper.make_dispatch_reply(
            msg, 'foo')

        resp1 = yield resp1_d
        reply1 = WeChatXMLParser.parse(resp1.delivered_body)
        self.assertTrue(isinstance(reply1, TextMessage))

    @inlineCallbacks
    def test_locking(self):
        transport1 = yield self.get_transport_with_access_token('foo')
        transport2 = yield self.get_transport_with_access_token('foo')
        transport3 = yield self.get_transport_with_access_token('foo')

        if any([isinstance(tx.redis._client, FakeRedis)
                for tx in [transport1, transport2, transport3]]):
            raise SkipTest(
                'FakeRedis setnx is not atomic. '
                'See https://github.com/praekelt/vumi/issues/789')

        locks = yield gatherResults([
            transport1.mark_as_seen_recently('msg-id'),
            transport2.mark_as_seen_recently('msg-id'),
            transport3.mark_as_seen_recently('msg-id'),
        ])
        self.assertEqual(sorted(locks), [0, 0, 1])
