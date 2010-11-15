import importlib
import os.path

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

