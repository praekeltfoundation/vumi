# -*- test-case-name: vumi.transports.wechat.tests.test_message_types -*-

import re
import json
from xml.etree.ElementTree import Element, SubElement, tostring, fromstring

from vumi.transports.wechat.errors import (
    WeChatParserException, WeChatException)


def get_child_value(node, name):
    [child] = node.findall(name)
    return (child.text.strip() if child.text is not None else '')


def append(node, tag, value):
    el = SubElement(node, tag)
    el.text = value


class WeChatMessage(object):

    mandatory_fields = ()
    optional_fields = ()

    @classmethod
    def from_xml(cls, doc):
        params = [get_child_value(doc, name)
                  for name in cls.mandatory_fields]

        for field in cls.optional_fields:
            try:
                params.append(get_child_value(doc, field))
            except ValueError:
                # element not present
                continue
        return cls(*params)


class TextMessage(WeChatMessage):

    mandatory_fields = (
        'ToUserName',
        'FromUserName',
        'CreateTime',
        'Content',
    )

    optional_fields = (
        'MsgId',
    )

    def __init__(self, to_user_name, from_user_name, create_time, content,
                 msg_id=None):
        self.to_user_name = to_user_name
        self.from_user_name = from_user_name
        self.create_time = create_time
        self.content = content
        self.msg_id = msg_id

    @classmethod
    def from_vumi_message(cls, message):
        md = message['transport_metadata'].get('wechat', {})
        from_addr = md.get('ToUserName', message['from_addr'])
        return cls(message['to_addr'], from_addr,
                   message['timestamp'].strftime('%s'),
                   message['content'])

    def to_xml(self):
        xml = Element('xml')
        append(xml, 'ToUserName', self.to_user_name)
        append(xml, 'FromUserName', self.from_user_name)
        append(xml, 'CreateTime', self.create_time)
        append(xml, 'MsgType', 'text')
        append(xml, 'Content', self.content)
        return tostring(xml)

    def to_json(self):
        return json.dumps({
            'touser': self.to_user_name,
            'msgtype': 'text',
            'text': {
                'content': self.content,
            }
        })


class NewsMessage(WeChatMessage):

    # Has something URL-ish in it
    URLISH = re.compile(
        r'(?P<before>.*)'
        r'(?P<url>http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)'
        r'(?P<after>.*?)')

    def __init__(self, to_user_name, from_user_name, create_time,
                 items=None):
        self.to_user_name = to_user_name
        self.from_user_name = from_user_name
        self.create_time = create_time
        self.items = ([] if items is None else items)

    @classmethod
    def accepts(cls, vumi_message):
        return cls.URLISH.match(vumi_message['content'])

    @classmethod
    def from_vumi_message(cls, match, vumi_message):
        md = vumi_message['transport_metadata'].get('wechat', {})
        from_addr = md.get('ToUserName', vumi_message['from_addr'])
        url_data = match.groupdict()
        return cls(
            vumi_message['to_addr'],
            from_addr,
            vumi_message['timestamp'].strftime('%s'),
            [{
                'title': '%(before)s' % url_data,
                'url': '%(url)s' % url_data,
                'description': vumi_message['content']
            }])

    def to_xml(self):
        xml = Element('xml')
        append(xml, 'ToUserName', self.to_user_name)
        append(xml, 'FromUserName', self.from_user_name)
        append(xml, 'CreateTime', self.create_time)
        append(xml, 'MsgType', 'news')
        append(xml, 'ArticleCount', str(len(self.items)))
        articles = SubElement(xml, 'Articles')
        for item in self.items:
            if not any(item.values()):
                raise WeChatException(
                    'News items must have some values.')

            item_element = SubElement(articles, 'item')
            if 'title' in item:
                append(item_element, 'Title', item['title'])
            if 'description' in item:
                append(item_element, 'Description', item['description'])
            if 'picurl' in item:
                append(item_element, 'PicUrl', item['picurl'])
            if 'url' in item:
                append(item_element, 'Url', item['url'])
        return tostring(xml)

    def to_json(self):
        return json.dumps({
            'touser': self.to_user_name,
            'msgtype': 'news',
            'news': {
                'articles': self.items
            }
        })


class EventMessage(WeChatMessage):

    mandatory_fields = (
        'ToUserName',
        'FromUserName',
        'CreateTime',
        'Event',
    )

    optional_fields = (
        'MsgId',
        'EventKey',
    )

    def __init__(self, to_user_name, from_user_name, create_time, event,
                 event_key=None):
        self.to_user_name = to_user_name
        self.from_user_name = from_user_name
        self.create_time = create_time
        self.event = event
        self.event_key = event_key


class WeChatXMLParser(object):

    ENCODING = 'utf-8'
    CLASS_MAP = {
        'text': TextMessage,
        'news': NewsMessage,
        'event': EventMessage,
    }

    @classmethod
    def parse(cls, string):
        doc = fromstring(string.decode(cls.ENCODING))
        klass = cls.get_class(doc)
        return klass.from_xml(doc)

    @classmethod
    def get_class(cls, doc):
        msg_types = doc.findall('MsgType')
        if not msg_types:
            raise WeChatParserException('No MsgType found.')

        if len(msg_types) > 1:
            raise WeChatParserException('More than 1 MsgType found.')

        [msg_type_element] = msg_types
        msg_type = msg_type_element.text.strip()
        if msg_type not in cls.CLASS_MAP:
            raise WeChatParserException(
                'Unsupported MsgType: %s' % (msg_type,))

        return cls.CLASS_MAP[msg_type]
