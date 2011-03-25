import json, re
from collections import namedtuple
from datetime import datetime

CLASS_CACHE = {}
TYPE_CAST_MAP = [
    (r'.*_at$', lambda i: datetime.strptime(i, '%Y-%m-%d %H:%M:%S')),
    (r'.*_id$', lambda i: int(i)),
]

class ClientException(Exception): pass

def camelcase(name):
    return ''.join(map(lambda s: s.capitalize(), name.split('_')))

def make_response(name, dictionary):
    if name not in CLASS_CACHE:
        cls = namedtuple(name, dictionary.keys())
        CLASS_CACHE[name] = cls
    typed_dictionary = process_types(dictionary)
    return CLASS_CACHE[name](**typed_dictionary)

def process_types(dictionary):
    for pattern, func in TYPE_CAST_MAP:
        keys = filter(lambda key: re.search(pattern, key), dictionary.keys())
        for key in keys:
            value = dictionary.get(key)
            if value:
                dictionary[key] = func(value)
    return dictionary

def auto_response(function):
    def decorator(self, **kwargs):
        response = function(self, **kwargs)
        obj = json.loads(response.content)
        name = "%sResponse" % camelcase(function.__name__)
        if isinstance(obj, list):
            return map(lambda o: make_response(name, o), obj)
        else:
            raise ClientException, 'response must be a list of dictionaries'
    return decorator

class Client(object):
    
    def __init__(self, connection):
        self.connection = connection
    
    @auto_response
    def send_sms(self, **kwargs):
        if 'to_msisdn' in kwargs:
            kwargs['to_msisdns'] = [kwargs.pop('to_msisdn')]
        return self.connection.post('/api/v1/sms/send.json', {
            'to_msisdn': kwargs.get('to_msisdns', []),
            'from_msisdn': kwargs['from_msisdn'],
            'message': kwargs['message']
        })
    
    @auto_response
    def send_template_sms(self, **kwargs):
        # make sure we order the entries consistently
        to_msisdns = kwargs.get('to_msisdns', {})
        entries = [(msisdn, context) for msisdn, context in to_msisdns.items()]
        kwargs = {
            'template': kwargs['template'],
            'from_msisdn': kwargs['from_msisdn'],
            'to_msisdn': [msisdn for msisdn, context in entries],
        }
        
        for msisdn, context in entries:
            for key, value in context.items():
                kwargs.setdefault('template_%s' % key, []).append(value)
        print kwargs
        return self.connection.post('/api/v1/sms/template_send.json', kwargs)