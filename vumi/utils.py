# -*- test-case-name: vumi.tests.test_utils -*-

import os.path
import re
import sys
import base64
import pkg_resources
import warnings
from functools import wraps

from zope.interface import implements
from twisted.internet import defer
from twisted.internet import reactor, protocol
from twisted.internet.defer import succeed
from twisted.python.failure import Failure
from twisted.web.client import Agent, ResponseDone, WebClientContextFactory
from twisted.web.server import Site
from twisted.web.http_headers import Headers
from twisted.web.iweb import IBodyProducer
from twisted.web.http import PotentialDataLoss
from twisted.web.resource import Resource

from vumi.errors import VumiError


# Stop Agent from logging two useless lines for every request.
# This is hacky, but there's no better way to do it for now.
from twisted.web import client
client._HTTP11ClientFactory.noisy = False


def import_module(name):
    """
    This is a simpler version of `importlib.import_module` and does
    not support relative imports.

    It's here so that we can avoid using importlib and not have to
    juggle different deps between Python versions.
    """
    __import__(name)
    return sys.modules[name]


def to_kwargs(kwargs):
    """
    Convert top-level keys from unicode to string for older Python versions.

    See http://bugs.python.org/issue2646 for details.
    """
    return dict((k.encode('utf8'), v) for k, v in kwargs.iteritems())


class HttpError(VumiError):
    """Base class for errors raised by http_request_full."""


class HttpDataLimitError(VumiError):
    """Returned by http_request_full if too much data is returned."""


class HttpTimeoutError(VumiError):
    """Returned by http_request_full if the request times out."""


class SimplishReceiver(protocol.Protocol):
    def __init__(self, response, data_limit=None):
        self.deferred = defer.Deferred(canceller=self.cancel_on_timeout)
        self.response = response
        self.data_limit = data_limit
        self.data_recvd_len = 0
        self.response.delivered_body = ''
        if response.code == 204:
            self.deferred.callback(self.response)
        else:
            response.deliverBody(self)

    def cancel_on_timeout(self, d):
        self.cancel_receiving(
            HttpTimeoutError("Timeout while receiving data")
        )

    def cancel_on_data_limit(self):
        self.cancel_receiving(
            HttpDataLimitError("More than %d bytes received"
                               % (self.data_limit,))
        )

    def cancel_receiving(self, err):
        self.transport.stopProducing()
        self.deferred.errback(err)

    def data_limit_exceeded(self):
        return (self.data_limit is not None and
                self.data_recvd_len > self.data_limit)

    def dataReceived(self, data):
        self.data_recvd_len += len(data)
        if self.data_limit_exceeded():
            self.cancel_on_data_limit()
        self.response.delivered_body += data

    def connectionLost(self, reason):
        if self.deferred.called:
            # this happens when the deferred is cancelled and this
            # triggers connection closing
            return
        if reason.check(ResponseDone):
            self.deferred.callback(self.response)
        elif reason.check(PotentialDataLoss):
            # This is only (and always!) raised if we have an HTTP 1.0 request
            # with no Content-Length.
            # See http://twistedmatrix.com/trac/ticket/4840 for sadness.
            #
            # We ignore this and treat the call as success. If we care about
            # checking for potential data loss, we should do that in all cases
            # rather than trying to figure out if we might need to.
            self.deferred.callback(self.response)
        else:
            self.deferred.errback(reason)


def http_request_full(url, data=None, headers={}, method='POST',
                      timeout=None, data_limit=None, context_factory=None,
                      agent_class=Agent):
    context_factory = context_factory or WebClientContextFactory()
    agent = agent_class(reactor, contextFactory=context_factory)
    d = agent.request(method,
                      url,
                      mkheaders(headers),
                      StringProducer(data) if data else None)

    def handle_response(response):
        return SimplishReceiver(response, data_limit).deferred

    d.addCallback(handle_response)

    if timeout is not None:
        cancelling_on_timeout = [False]

        def raise_timeout(reason):
            if not cancelling_on_timeout[0]:
                return reason
            return Failure(HttpTimeoutError("Timeout while connecting"))

        def cancel_on_timeout():
            cancelling_on_timeout[0] = True
            d.cancel()

        def cancel_timeout(r, delayed_call):
            if delayed_call.active():
                delayed_call.cancel()
            return r

        d.addErrback(raise_timeout)
        delayed_call = reactor.callLater(timeout, cancel_on_timeout)
        d.addCallback(cancel_timeout, delayed_call)

    return d


def mkheaders(headers):
    """
    Turn a dict of HTTP headers into an instance of Headers.

    Twisted expects a list of values, not a single value. We should
    support both.
    """
    raw_headers = {}
    for k, v in headers.iteritems():
        if isinstance(v, basestring):
            v = [v]
        raw_headers[k] = v
    return Headers(raw_headers)


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

    raw = ''.join([c for c in raw if c.isdigit() or c == '+'])
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


def build_web_site(resources, site_class=None):
    """Build a Twisted web Site instance for a specified dictionary of
    resources.

    :param dict resources:
        Dictionary of path -> resource class mappings to create the site from.
    :type site_class: Sub-class of Twisted's Site
    :param site_class:
        Site class to create. Defaults to :class:`LogFilterSite`.
    """
    if site_class is None:
        site_class = LogFilterSite

    root = Resource()
    # sort by ascending path length to make sure we create
    # resources lower down in the path earlier
    resources = resources.items()
    resources = sorted(resources, key=lambda r: len(r[0]))

    def create_node(node, path):
        if path in node.children:
            return node.children.get(path)
        else:
            new_node = Resource()
            node.putChild(path, new_node)
            return new_node

    for path, resource in resources:
        request_path = filter(None, path.split('/'))
        nodes, leaf = request_path[0:-1], request_path[-1]
        parent = reduce(create_node, nodes, root)
        parent.putChild(leaf, resource)

    site_factory = site_class(root)
    return site_factory


class LogFilterSite(Site):
    def log(self, request):
        if getattr(request, 'do_not_log', None):
            return
        return Site.log(self, request)


class PkgResources(object):
    """
    A helper for accessing a packages data files.

    :param str modname:
        The full dotted name of the module. E.g.
        ``vumi.resources``.
    """
    def __init__(self, modname):
        self.modname = modname

    def path(self, path):
        """
        Return the absolute path to a package resource.

        If path is already absolute, it is returned unmodified.

        :param str path:
            The relative or absolute path to the resource.
        """
        if os.path.isabs(path):
            return path
        return pkg_resources.resource_filename(self.modname, path)


vumi_resource_path = PkgResources("vumi.resources").path


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


def redis_from_config(redis_config):
    """
    Return a redis client instance from a config.

    If redis_config:

    * equals 'FAKE_REDIS', a new instance of :class:`FakeRedis` is returned.
    * is an instance of :class:`FakeRedis` that instance is returned

    Otherwise a new real redis client is returned.
    """
    warnings.warn("Use of redis directly is deprecated. Use vumi.persist "
                  "instead.", category=DeprecationWarning)

    import redis
    from vumi.persist import fake_redis
    if redis_config == "FAKE_REDIS":
        return fake_redis.FakeRedis()
    if isinstance(redis_config, fake_redis.FakeRedis):
        return redis_config
    return redis.Redis(**redis_config)


def flatten_generator(generator_func):
    """
    This is a synchronous version of @inlineCallbacks.

    NOTE: It doesn't correctly handle returnValue() being called in a
    non-decorated function called from the function we're decorating. We could
    copy the Twisted code to do that, but it's messy.
    """
    @wraps(generator_func)
    def wrapped(*args, **kw):
        gen = generator_func(*args, **kw)
        result = None
        while True:
            try:
                result = gen.send(result)
            except StopIteration:
                # Fell off the end, or "return" statement.
                return None
            except defer._DefGen_Return, e:
                # returnValue() called.
                return e.value

    return wrapped


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


def get_first_word(content, delimiter=' '):
    """
    Returns the first word from a string.

    Example::

      >>> get_first_word('KEYWORD rest of message')
      'KEYWORD'

    :type content: str or None
    :param content:
        Content from which the first word will be retrieved. If the
        content is None it is treated as an empty string (this is a
        convenience for dealing with content-less messages).
    :param str delimiter:
        Delimiter to split the string on. Default is ' '.
        Passed to :func:`string.partition`.
    :returns:
        A string containing the first word.
    """
    return (content or '').partition(delimiter)[0]


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


def generate_worker_id(system_id, worker_id):
    return "%s:%s" % (system_id, worker_id,)
