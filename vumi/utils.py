# -*- test-case-name: vumi.tests.test_utils -*-

import os.path
import re
import sys
import base64

import pkg_resources
from zope.interface import implements
from twisted.internet import defer
from twisted.internet import reactor, protocol
from twisted.internet.defer import succeed
from twisted.web.client import Agent, ResponseDone
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer


def import_module(name):
    """
    This is a simpler version of `importlib.import_module` and does
    not support relative imports.

    It's here so that we can avoid using importlib and not have to
    juggle different deps between Python versions.
    """
    __import__(name)
    return sys.modules[name]


class SimplishReceiver(protocol.Protocol):
    def __init__(self, response):
        self.deferred = defer.Deferred()
        self.response = response
        self.response.delivered_body = ''
        if response.code == 204:
            self.deferred.callback(self.response)
        else:
            response.deliverBody(self)

    def dataReceived(self, data):
        self.response.delivered_body += data

    def connectionLost(self, reason):
        if reason.check(ResponseDone):
            self.deferred.callback(self.response)
        else:
            self.deferred.errback(reason)


def http_request_full(url, data=None, headers={}, method='POST'):
    agent = Agent(reactor)
    d = agent.request(method,
                      url,
                      Headers(headers),
                      StringProducer(data) if data else None)

    def handle_response(response):
        return SimplishReceiver(response).deferred

    d.addCallback(handle_response)
    return d


def http_request(url, data, headers={}, method='POST'):
    d = http_request_full(url, data, headers=headers, method=method)
    return d.addCallback(lambda r: r.delivered_body)


def basic_auth_string(username, password):
    """
    Encode a username and password for use in an HTTP Basic Authentication
    header
    """
    b64 = base64.encodestring('%s:%s' % (username, password)).strip()
    return 'Basic %s' % b64


def normalize_msisdn(raw, country_code=''):
    # don't touch shortcodes
    if len(raw) <= 5:
        return raw

    raw = ''.join([c for c in str(raw) if c.isdigit() or c == '+'])
    if raw.startswith('00'):
        return '+' + raw[2:]
    if raw.startswith('0'):
        return '+' + country_code + raw[1:]
    if raw.startswith('+'):
        return raw
    if raw.startswith(country_code):
        return '+' + raw
    return raw


class StringProducer(object):
    """
    For various twisted.web mechanics we need a producer to produce
    content for HTTP requests, this is a helper class to quickly
    create a producer for a bit of content
    """
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


def vumi_resource_path(path):
    """
    Return an absolute path to a Vumi package resource.

    Vumi package resources are found in the vumi.resources package.
    If the path is already absolute, it is returned unmodified.
    """
    if os.path.isabs(path):
        return path
    return pkg_resources.resource_filename("vumi.resources", path)


def load_class(module_name, class_name):
    """
    Load a class when given its module and its class name

    >>> load_class('vumi.workers.example','ExampleWorker') # doctest: +ELLIPSIS
    <class vumi.workers.example.ExampleWorker at ...>
    >>>

    """
    mod = import_module(module_name)
    return getattr(mod, class_name)


def load_class_by_string(class_path):
    """
    Load a class when given its full name, including modules in python
    dot notation

    >>> cls = 'vumi.workers.example.ExampleWorker'
    >>> load_class_by_string(cls) # doctest: +ELLIPSIS
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
    return dict((key.split(delimiter, 1)[1], value)
                for key, value in options.items()
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
    number = numbers.get(operator)
    return number


def safe_routing_key(routing_key):
    """
    >>> safe_routing_key(u'*32323#')
    u's32323h'
    >>>

    """
    return reduce(lambda r_key, kv: r_key.replace(*kv),
                    [('*', 's'), ('#', 'h')], routing_key)


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


def get_deploy_int(deployment):
    lookup = {
        "develop": 7,
        "/develop": 7,
        "development": 7,
        "/development": 7,
        "production": 8,
        "/production": 8,
        "staging": 9,
        "/staging": 9,
        "qa": 9,
        "/qa": 9,
        }
    return lookup.get(deployment.lower(), 7)
