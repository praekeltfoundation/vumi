import importlib
import os.path
import re

def load_class(module_name, class_name):
    """
    Load a class when given its module and its class name
    
    >>> load_class('vumi.workers.base','VumiWorker')
    <class 'base.VumiWorker'>
    >>> 
    
    """
    mod = importlib.import_module(module_name)
    return getattr(mod, class_name)

def load_class_by_string(class_path):
    """
    Load a class when given it's full name, including modules in python
    dot notation
    
    >>> load_class_by_string('vumi.workers.base.VumiWorker')
    <class 'base.VumiWorker'>
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

