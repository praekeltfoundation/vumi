from datetime import datetime

from twisted.trial.unittest import TestCase

from vumi.transports.wechat import message_types, new_message_types
from vumi.transports.wechat.parser import WeChatParser, WeChatXMLParser
from vumi.transports.wechat.errors import WeChatParserException


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
        self.assertEqual(event.EventKey, '')
        self.assertTrue(isinstance(event, message_types.EventMessage))


class TestWeChatXMLParser(TestCase):

    def test_missing_msg_type(self):
        self.assertRaises(
            WeChatParserException, WeChatXMLParser.parse, '<xml/>')

    def test_multiple_msg_types(self):
        self.assertRaises(
            WeChatParserException, WeChatXMLParser.parse,
            '<xml><MsgType>Foo</MsgType><MsgType>Bar</MsgType></xml>')

    def test_text_message_parse(self):
        msg = WeChatXMLParser.parse(
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

        self.assertEqual(msg.to_user_name, 'toUser')
        self.assertEqual(msg.from_user_name, 'fromUser')
        self.assertEqual(msg.create_time, '1348831860')
        self.assertEqual(msg.msg_id, '1234567890123456')
        self.assertEqual(msg.content, 'this is a test')
        self.assertTrue(isinstance(msg, new_message_types.TextMessage))

    def test_text_message_to_xml(self):
        msg = new_message_types.TextMessage(
            'to_addr', 'from_addr', '1348831860', 'this is a test')
        self.assertEqual(
            msg.to_xml(),
            "".join([
            "<xml>"
            "<ToUserName>to_addr</ToUserName>"
            "<FromUserName>from_addr</FromUserName>"
            "<CreateTime>1348831860</CreateTime>"
            "<MsgType>text</MsgType>"
            "<Content>this is a test</Content>"
            "</xml>"
            ]))

    def test_news_message_to_xml(self):
        msg = new_message_types.NewsMessage(
            'to_addr', 'from_addr', '1348831860', [{
                'title': 'title1',
                'description': 'description1',
            }, {
                'pic_url': 'picurl',
                'url': 'url',
            }])
        self.assertEqual(
            msg.to_xml(),
            ''.join([
                "<xml>",
                "<ToUserName>to_addr</ToUserName>",
                "<FromUserName>from_addr</FromUserName>",
                "<CreateTime>1348831860</CreateTime>",
                "<MsgType>news</MsgType>",
                "<ArticleCount>2</ArticleCount>",
                "<Articles>",
                "<item>",
                "<Title>title1</Title>",
                "<Description>description1</Description>",
                "</item>",
                "<item>",
                "<PicUrl>picurl</PicUrl>",
                "<Url>url</Url>",
                "</item>",
                "</Articles>",
                "</xml>",
            ]))

    def test_event_message_parse(self):
        msg = WeChatXMLParser.parse(
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
        self.assertEqual(msg.to_user_name, 'toUser')
        self.assertEqual(msg.from_user_name, 'fromUser')
        self.assertEqual(msg.create_time, '1395130515')
        self.assertEqual(msg.event, 'subscribe')
        self.assertEqual(msg.event_key, '')
