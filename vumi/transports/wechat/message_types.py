import re

from xml.etree.ElementTree import Element, SubElement, tostring


class Message(object):

    field_types = []
    msg_type = None

    def __init__(self, data):
        self.fields = []
        for field in self.field_types:
            if field.name in data:
                field = Field(field.name, value=data[field.name],
                              timestamp=field.timestamp)
                self.fields.append(field)

    def build_field(self, parent, field):
        node = SubElement(parent, field.name)
        if isinstance(field.value, list):
            for child in field.value:
                for child_name, values in child.items():
                    field = Field(child_name, value=values)
                    self.build_field(node, field)

        elif field.timestamp:
            node.text = field.value.strftime('%s')
        else:
            node.text = field.value

    def to_xml(self):
        xml = Element('xml')
        for field in self.fields:
            self.build_field(xml, field)

        msg_type = SubElement(xml, 'MsgType')
        msg_type.text = self.msg_type
        return tostring(xml)

    def __getattr__(self, attr):
        for field in self.fields:
            if field.name == attr:
                return field.value

    def __eq__(self, other):
        return (self.msg_type == other.msg_type and
                [(f.name, f.value) for f in self.fields] ==
                [(f.name, f.value) for f in other.fields])


class Field(object):

    def __init__(self, name, timestamp=False, value=None, children=[]):
        self.name = name
        self.timestamp = timestamp
        self.value = value
        self.children = children


class TextMessage(Message):

    msg_type = 'text'
    field_types = [
        Field('ToUserName'),
        Field('FromUserName'),
        Field('CreateTime', timestamp=True),
        Field('Content'),
        Field('MsgId'),
    ]

    @classmethod
    def build(cls, message):
        metadata = message['transport_metadata']['wechat']
        return cls({
            'ToUserName': metadata['FromUserName'],
            'FromUserName': metadata['ToUserName'],
            'CreateTime': message['timestamp'],
            'Content': message['content']
        })


class RichMediaMessage(Message):

    msg_type = 'news'  # WAT?! WeChat?
    field_types = [
        Field('ToUserName'),
        Field('FromUserName'),
        Field('CreateTime', timestamp=True),
        Field('ArticleCount'),
        Field('Articles', children=[
            Field('item', children=[
                Field('Title'),
                Field('Description'),
                Field('PicUrl'),
                Field('Url'),
            ])
        ]),
    ]

    # Has something URL-ish in it
    URLISH = re.compile(
        r'(?P<before>.*)'
        r'(?P<schema>[a-zA-Z]{4,5})\://'
        r'(?P<domain>[^\s]+)'
        r'(?P<after>.*)')

    @classmethod
    def accepts(cls, msg):
        return cls.URLISH.match(msg['content'])

    @classmethod
    def build(cls, match, message):
        url_data = match.groupdict()
        metadata = message['transport_metadata']['wechat']
        return cls({
            'ToUserName': metadata['FromUserName'],
            'FromUserName': metadata['ToUserName'],
            'CreateTime': message['timestamp'],
            'ArticleCount': '1',
            'Articles': [{
                'item': [{
                    'Url': '%(schema)s://%(domain)s' % url_data,
                    'Description': '%(before)s%(after)s' % url_data,
                }]
            }]
        })


class EventMessage(Message):

    msg_type = 'event'
    field_types = [
        Field('ToUserName'),
        Field('FromUserName'),
        Field('CreateTime', timestamp=True),
        Field('Event'),
        Field('EventType'),
        Field('EventKey'),
    ]
