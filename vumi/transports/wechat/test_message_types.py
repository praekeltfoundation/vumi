import json

from twisted.trial.unittest import TestCase

from vumi.transports.wechat.message_types import (
    WeChatXMLParser, TextMessage, NewsMessage)
from vumi.transports.wechat.errors import WeChatParserException


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
        self.assertTrue(isinstance(msg, TextMessage))

    def test_text_message_to_xml(self):
        msg = TextMessage(
            'to_addr', 'from_addr', '1348831860', 'this is a test')
        self.assertEqual(
            msg.to_xml(),
            "".join([
                "<xml>",
                "<ToUserName>to_addr</ToUserName>",
                "<FromUserName>from_addr</FromUserName>",
                "<CreateTime>1348831860</CreateTime>",
                "<MsgType>text</MsgType>",
                "<Content>this is a test</Content>",
                "</xml>",
            ]))

    def test_text_message_to_json(self):
        msg = TextMessage(
            'to_addr', 'from_addr', '1348831860', 'this is a test')
        self.assertEqual(
            json.loads(msg.to_json()),
            {
                'touser': 'to_addr',
                'msgtype': 'text',
                'text': {
                    'content': 'this is a test'
                }
            })

    def test_news_message_to_xml(self):
        msg = NewsMessage(
            'to_addr', 'from_addr', '1348831860', [{
                'title': 'title1',
                'description': 'description1',
            }, {
                'picurl': 'picurl',
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

    def test_news_message_to_json(self):
        msg = NewsMessage(
            'to_addr', 'from_addr', '1348831860', [{
                'title': 'title1',
                'description': 'description1',
            }, {
                'picurl': 'picurl',
                'url': 'url',
            }])
        self.assertEqual(
            json.loads(msg.to_json()),
            {
                'touser': 'to_addr',
                'msgtype': 'news',
                'news': {
                    'articles': [{
                        'title': 'title1',
                        'description': 'description1'
                    }, {
                        'picurl': 'picurl',
                        'url': 'url'
                    }]
                }
            })

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
