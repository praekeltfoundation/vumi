# -*- test-case-name: vumi.transports.wechat.tests.test_parser -*-
from twisted.web import microdom

from vumi.transports.wechat import message_types
from vumi.transports.wechat.errors import WeChatParserException


class WeChatXMLParser(object):

    ENCODING = 'utf-8'
    CLASS_MAP = {
        'text': message_types.TextMessage,
        'news': message_types.NewsMessage,
        'event': message_types.EventMessage,
    }

    @classmethod
    def parse_doc(cls, string):
        return microdom.parseXMLString(string.decode(cls.ENCODING))

    @classmethod
    def parse(cls, string):
        doc = cls.parse_doc(string)
        klass = cls.get_class(doc)
        return klass.from_xml(doc)

    @classmethod
    def get_class(cls, doc):
        root = doc.firstChild()
        msg_types = root.getElementsByTagName('MsgType')
        if not msg_types:
            raise WeChatParserException('No MsgType found.')

        if len(msg_types) > 1:
            raise WeChatParserException('More than 1 MsgType found.')

        [msg_type_element] = msg_types
        msg_type = ''.join([child.value.lower()
                            for child in msg_type_element.childNodes])
        if msg_type not in cls.CLASS_MAP:
            raise WeChatParserException(
                'Unsupported MsgType: %s' % (msg_type,))

        return cls.CLASS_MAP[msg_type]
