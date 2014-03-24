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

    def to_xml(self):
        xml = Element('xml')
        for field in self.fields:
            node = SubElement(xml, field.name)
            if field.timestamp:
                node.text = field.value.strftime('%s')
            else:
                node.text = field.value

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

    def __init__(self, name, timestamp=False, value=None):
        self.name = name
        self.timestamp = timestamp
        self.value = value


class TextMessage(Message):

    msg_type = 'text'
    field_types = [
        Field('ToUserName'),
        Field('FromUserName'),
        Field('CreateTime', timestamp=True),
        Field('Content'),
        Field('MsgId'),
    ]


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
