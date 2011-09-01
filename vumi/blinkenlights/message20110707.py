# -*- test-case-name: vumi.blinkenlights.tests.test_message20110707 -*-

from datetime import datetime

from vumi import message as vumi_message


class Message(object):
    """
    Blinkenlights message object. This sits inside a Vumi message, and
    works with decoded JSON data.
    """

    VERSION = "20110707"
    MESSAGE_TYPE = None
    REQUIRED_FIELDS = (
        # Excludes message_version, which is handled differently.
        "message_type",
        "source_name",
        "source_id",
        "payload",
        "timestamp",
        )

    def __init__(self, message_type, source_name, source_id, payload,
                 timestamp=None):
        self.source_name = source_name
        self.source_id = source_id
        self.message_type = message_type
        self.payload = payload
        if timestamp is None:
            timestamp = datetime.utcnow()
        if not isinstance(timestamp, datetime):
            # Assume it's a list or tuple here
            timestamp = datetime(*timestamp)
        self.timestamp = timestamp
        if self.MESSAGE_TYPE and self.MESSAGE_TYPE != self.message_type:
            raise ValueError("Incorrect message type. Expected '%s', got"
                             " '%s'." % (self.MESSAGE_TYPE, self.message_type))
        self.process_payload()

    def process_payload(self):
        pass

    def to_dict(self):
        message = {'message_version': self.VERSION}
        message.update(dict((field, getattr(self, field))
                            for field in self.REQUIRED_FIELDS))
        # Massage the timestamp into the serialised list we use
        message['timestamp'] = list(self.timestamp.timetuple()[:6])
        return message

    def to_vumi_message(self):
        return vumi_message.Message(**self.to_dict())

    @classmethod
    def from_dict(cls, message):
        message = message.copy()  # So we can modify it safely
        version = message.pop('message_version')
        if version != cls.VERSION:
            raise ValueError("Incorrect message version. Expected '%s', got"
                             " '%s'." % (cls.VERSION, version))
        for field in cls.REQUIRED_FIELDS:
            if field not in message:
                raise ValueError("Missing mandatory field '%s'." % (field,))
        for field in message:
            if field not in cls.REQUIRED_FIELDS:
                raise ValueError("Found unexpected field '%s'." % (field,))
        if not message['timestamp']:
            raise ValueError("Missing timestamp in field 'timestamp'.")
        return cls(**message)

    def __str__(self):
        return u"<Message v%s:%s %s src=(%s, %s) payload=\"%s\">" % (
            self.VERSION, self.message_type, self.timestamp,
            self.source_name, self.source_id, repr(self.payload))

    def __eq__(self, other):
        if self.VERSION != other.VERSION:
            return False
        if self.REQUIRED_FIELDS != other.REQUIRED_FIELDS:
            return False
        for field in self.REQUIRED_FIELDS:
            if getattr(self, field) != getattr(other, field):
                return False
        return True


class MetricsMessage(Message):
    MESSAGE_TYPE = "metrics"

    def process_payload(self):
        self.metrics = {}

        for metric in self.payload:
            name = metric['name']
            count = metric['count']
            time = metric.get('time', None)
            tags = dict(i for i in metric.items()
                        if i[0] not in ('name', 'count', 'time'))
            self.metrics.setdefault(name, []).append((count, time, tags))
