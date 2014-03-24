from twisted.web import microdom
from datetime import datetime

from vumi.transports.wechat import message_types
from vumi.transports.wechat.errors import UnsupportedWechatMessage


FIELD_PARSERS = {
    'CreateTime': lambda data: datetime.fromtimestamp(int(data)),
    'MsgType': lambda data: getattr(message_types,
                                    '%sMessage' % (data.capitalize(),)),
}
IDENTITY = lambda data: data


def get_cdata(node):
    value = ''
    for child in node.childNodes:
        value += child.value
    parser = FIELD_PARSERS.get(node.nodeName, IDENTITY)
    try:
        return parser(value)
    # raised when we try & get a MsgType from the message_types module
    except AttributeError:
        raise UnsupportedWechatMessage(
            'Unparseable WeChat Message Element %r: %r' % (
                node.nodeName, value,))


class WeChatParser(object):

    ENCODING = 'utf-8'
    TEXT = 'text'

    @classmethod
    def parse(cls, string):
        document = microdom.parseXMLString(string.decode(cls.ENCODING))
        root = document.firstChild()
        data = dict([(node.nodeName, get_cdata(node))
                     for node in root.childNodes])
        message_class = data.pop('MsgType')
        return message_class(data)
