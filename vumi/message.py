import json, datetime

class Message(object):
    """
    Start of a somewhat unified message object to be
    used internally in Vumi and while being in transit
    over AMQP

    scary transport format -> Vumi Tansport -> Unified Message -> Vumi Worker
    
    """

    def __init__(self, **kwargs):
        self.payload = kwargs

    def to_json(self):
        return json.dumps(self.payload, cls=JSONMessageEncoder)
    
    @classmethod
    def from_json(klass, json_string):
        dictionary = json.loads(json_string, cls=JSONMessageDecoder)
        return klass(**dictionary)

    def __str__(self):
        return u"<Message payload=\"%s\">" % self.payload

class JSONMessageDecoder(json.JSONDecoder):
    """A JSON decoder that is ablo to read datetime values"""
    def decode(self, s):
        try:
            return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f')
        except ValueError, e:
            return super(JSONMessageDecoder, self).decode(s)
    

class JSONMessageEncoder(json.JSONEncoder):
    """A JSON encoder that is able to serialize datetime"""
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super(JSONEncoder, self).default(obj)
    

