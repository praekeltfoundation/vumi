import importlib
import os.path
import re

def load_class(module_name, class_name):
    """
    Load a class when given its module and its class name
    
    >>> load_class('vumi.workers.example','ExampleWorker') # doctest: +ELLIPSIS
    <class vumi.workers.example.ExampleWorker at ...>
    >>> 
    
    """
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)

def load_class_by_string(class_path):
    """
    Load a class when given it's full name, including modules in python
    dot notation
    
    >>> load_class_by_string('vumi.workers.example.ExampleWorker') # doctest: +ELLIPSIS
    <class vumi.workers.example.ExampleWorker at ...>
    >>> 
    
    """
    parts = class_path.split('.')
    module_name = '.'.join(parts[:-1])
    class_name = parts[-1]
    return load_class(module_name, class_name)


def filter_options_on_prefix(options, prefix, delimiter='-'):
    """
    splits an options dict based on key prefixes
    
    >>> filter_options_on_prefix({'foo-bar-1': 'ok'}, 'foo')
    {'bar-1': 'ok'}
    >>> 
    
    """
    return dict((key.split(delimiter,1)[1], value) \
                for key, value in options.items() \
                if key.startswith(prefix))



def cleanup_msisdn(number, country_code):
    number = re.sub('\+', '', number)
    number = re.sub('^0', country_code, number)
    return number

def get_operator_name(msisdn, mapping):
    for key, value in mapping.items():
        if msisdn.startswith(str(key)):
            if isinstance(value, dict):
                return get_operator_name(msisdn, value)
            return value
    return 'UNKNOWN'

def get_operator_number(msisdn, country_code, mapping, numbers):
    msisdn = cleanup_msisdn(msisdn, country_code)
    operator = get_operator_name(msisdn, mapping)
    number = numbers.get(operator, '')
    return number

def safe_routing_key(routing_key):
    """
    >>> safe_routing_key(u'*32323#')
    u's32323h'
    >>>
    
    """
    return reduce(lambda r_key, kv: r_key.replace(*kv), 
                    [('*','s'), ('#','h')], routing_key)


class TestPublisher(object):
    """
    A test publisher that caches outbound messages in an internal queue
    for testing, instead of publishing over AMQP.

    Useful for testing consumers
    """
    def __init__(self):
        self.queue = []
    
    def publish_message(self, message, **kwargs):
        self.queue.append((message, kwargs))
    


### SAMPLE CONFIG PARAMETERS - REPLACE 'x's IN OPERATOR_NUMBER

"""
COUNTRY_CODE: "27"

OPERATOR_NUMBER:
    VODACOM: "2782xxxxxxxxxxx"
    MTN: "2783xxxxxxxxxxx"
    CELLC: "2784xxxxxxxxxxx"
    VIRGIN: ""
    8TA: ""
    UNKNOWN: ""

OPERATOR_PREFIX:
    2771:
        27710: MTN
        27711: VODACOM
        27712: VODACOM
        27713: VODACOM
        27714: VODACOM
        27715: VODACOM
        27716: VODACOM
        27717: MTN
        27719: MTN

    2772: VODACOM
    2773: MTN
    2774:
        27740: CELLC
        27741: VIRGIN
        27742: CELLC
        27743: CELLC
        27744: CELLC
        27745: CELLC
        27746: CELLC
        27747: CELLC
        27748: CELLC
        27749: CELLC

    2776: VODACOM
    2778: MTN
    2779: VODACOM
    2781:
        27811: 8TA
        27812: 8TA
        27813: 8TA
        27814: 8TA

    2782: VODACOM
    2783: MTN
    2784: CELLC

"""
