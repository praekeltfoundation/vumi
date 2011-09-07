# -*- test-case-name: vumi.tests.test_message -*-

import json
from datetime import datetime

from errors import MissingMessageField, InvalidMessageField


VUMI_DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
"""This is the date format we work with internally"""


class Message(object):
    """
    Start of a somewhat unified message object to be
    used internally in Vumi and while being in transit
    over AMQP

    scary transport format -> Vumi Tansport -> Unified Message -> Vumi Worker

    """

    def __init__(self, **kwargs):
        self.payload = self.process_fields(kwargs)
        self.validate_fields()

    def process_fields(self, fields):
        return fields

    def validate_fields(self):
        pass

    def assert_field_present(self, *fields):
        for field in fields:
            if field not in self.payload:
                raise MissingMessageField(field)

    def assert_field_value(self, field, *values):
        self.assert_field_present(field)
        if self.payload[field] not in values:
            raise InvalidMessageField(field)

    def to_json(self):
        return json.dumps(self.payload, cls=JSONMessageEncoder)

    @classmethod
    def from_json(cls, json_string):
        dictionary = json.loads(json_string, object_hook=date_time_decoder)
        return cls(**dictionary)

    def __str__(self):
        return u"<Message payload=\"%s\">" % repr(self.payload)

    def __repr__(self):
        return str(self)

    def __eq__(self, other):
        return self.payload == other.payload

    def __getitem__(self, key):
        return self.payload[key]

    def __setitem__(self, key, value):
        self.payload[key] = value


def date_time_decoder(json_object):
    for key, value in json_object.items():
        try:
            json_object[key] = datetime.strptime(value,
                    VUMI_DATE_FORMAT)
        except ValueError:
            continue
        except TypeError:
            continue
    return json_object


class JSONMessageEncoder(json.JSONEncoder):
    """A JSON encoder that is able to serialize datetime"""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.strftime(VUMI_DATE_FORMAT)
        return super(json.JSONEncoder, self).default(obj)


class TransportMessage(Message):
    def process_fields(self, fields):
        fields.setdefault('timestamp', datetime.utcnow())
        fields.setdefault('message_version', '20110907')
        fields.setdefault('metadata', {})
        fields.setdefault('transport_metadata', {})
        fields.setdefault('message', '')
        return fields

    def validate_fields(self):
        self.assert_field_present(
            'message_version',
            'message_type',
            'message_id',
            'to_addr',
            'from_addr',
            'message',
            'metadata',
            'transport',
            'transport_metadata',
            'timestamp',
            )


class TransportSMS(TransportMessage):
    def process_fields(self, fields):
        fields = super(TransportSMS, self).process_fields(fields)
        fields['message_type'] = 'sms'
        return fields


class TransportSMSAck(TransportMessage):
    def process_fields(self, fields):
        fields = super(TransportSMSAck, self).process_fields(fields)
        fields['message_type'] = 'sms_ack'
        return fields

    def validate_fields(self):
        super(TransportSMSAck, self).validate_fields
        self.assert_field_present('transport_message_id')


class TransportSMSAck(TransportMessage):
    def process_fields(self, fields):
        fields = super(TransportSMSAck, self).process_fields(fields)
        fields['message_type'] = 'sms_ack'
        return fields

    def validate_fields(self):
        super(TransportSMSAck, self).validate_fields
        self.assert_field_present('transport_message_id')


class TransportSMSDeliveryReport(TransportMessage):
    def process_fields(self, fields):
        fields = super(TransportSMSDeliveryReport, self).process_fields(fields)
        fields['message_type'] = 'sms_delivery_report'
        return fields

    def validate_fields(self):
        super(TransportSMSDeliveryReport, self).validate_fields
        self.assert_field_present('transport_message_id')
        self.assert_field_value('delivery_status',
                                'pending', 'failed', 'delivered')
