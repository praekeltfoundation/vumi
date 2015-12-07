# -*- test-case-name: vumi.tests.test_testutils -*-

import re
from datetime import datetime, timedelta
import warnings
from functools import wraps

import pytz
from twisted.internet import reactor
from twisted.web.resource import Resource
from twisted.internet.defer import DeferredQueue, inlineCallbacks
from twisted.python import log

from vumi.service import get_spec, WorkerCreator
from vumi.tests.fake_amqp import FakeAMQClient
from vumi.utils import vumi_resource_path, flatten_generator, LogFilterSite

# For backcompat:
from vumi.tests.helpers import import_filter, import_skip
import_filter, import_skip  # To keep pyflakes happy.


class UTCNearNow(object):
    def __init__(self, offset=10):
        self.now = datetime.utcnow()
        self.utcnow = self.now.replace(tzinfo=pytz.UTC)
        self.offset = timedelta(offset)

    def __eq__(self, other):
        now = self.now
        if other.tzinfo:
            now = self.utcnow
        return (now - self.offset) < other < (now + self.offset)


class RegexMatcher(object):
    def __init__(self, regex):
        self.regex = re.compile(regex)

    def __eq__(self, other):
        return self.regex.match(other)


def get_fake_amq_client(broker=None):
    spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
    return FakeAMQClient(spec, {}, broker)


def get_stubbed_worker(worker_class, config=None, broker=None):
    worker = worker_class({}, config)
    worker._amqp_client = get_fake_amq_client(broker)
    return worker


class StubbedWorkerCreator(WorkerCreator):
    broker = None

    def _connect(self, worker, timeout, bindAddress):
        amq_client = get_fake_amq_client(self.broker)
        self.broker = amq_client.broker  # So we use the same broker for all.
        reactor.callLater(0, worker._amqp_connected, amq_client)


def FakeRedis():
    warnings.warn("Use of FakeRedis is deprecated. "
                  "Use persist.tests.fake_redis instead.",
                  category=DeprecationWarning)
    from vumi.persist import fake_redis
    return fake_redis.FakeRedis()


class LogCatcher(object):
    """Context manager for gathering logs in tests.

    :param str system:
        Only log events whose 'system' value contains the given
        regular expression pattern will be gathered. Default: None
        (i.e. keep all log events).

    :param str message:
        Only log events whose message contains the given regular
        expression pattern will be gathered. The message is
        constructed by joining the elements in the 'message' value
        with a space (the same way Twisted does). Default: None
        (i.e. keep all log events).

    :param int log_level:
        Only log events whose logLevel is equal to the given level
        will be gathered. Default: None (i.e. keep all log events).
    """

    def __init__(self, system=None, message=None, log_level=None):
        self.logs = []
        self.system = re.compile(system) if system is not None else None
        self.message = re.compile(message) if message is not None else None
        self.log_level = log_level

    @property
    def errors(self):
        return [ev for ev in self.logs if ev["isError"]]

    def messages(self):
        return [" ".join(msg['message']) for msg in self.logs
                if not msg["isError"]]

    def _keep_log(self, event_dict):
        if self.system is not None:
            if not self.system.search(event_dict.get('system', '-')):
                return False
        if self.message is not None:
            log_message = " ".join(event_dict.get('message', []))
            if not self.message.search(log_message):
                return False
        if self.log_level is not None:
            if event_dict.get('logLevel', None) != self.log_level:
                return False
        return True

    def _gather_logs(self, event_dict):
        if self._keep_log(event_dict):
            self.logs.append(event_dict)

    def __enter__(self):
        log.theLogPublisher.addObserver(self._gather_logs)
        return self

    def __exit__(self, *exc_info):
        log.theLogPublisher.removeObserver(self._gather_logs)


class MockResource(Resource):
    isLeaf = True

    def __init__(self, handler):
        Resource.__init__(self)
        self.handler = handler

    def render_GET(self, request):
        return self.handler(request)

    def render_POST(self, request):
        return self.handler(request)

    def render_PUT(self, request):
        return self.handler(request)


class MockHttpServer(object):
    """
    NOTE: This is deprecated.
          Please use :class:`vumi.tests.http_helpers.MockHttpHelper` instead.
    """

    def __init__(self, handler=None):
        self.queue = DeferredQueue()
        self._handler = handler or self.handle_request
        self._webserver = None
        self.addr = None
        self.url = None

    def handle_request(self, request):
        self.queue.put(request)

    @inlineCallbacks
    def start(self):
        root = MockResource(self._handler)
        site_factory = LogFilterSite(root)
        self._webserver = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        self.addr = self._webserver.getHost()
        self.url = "http://%s:%s/" % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def stop(self):
        yield self._webserver.stopListening()
        yield self._webserver.loseConnection()


def maybe_async(sync_attr):
    """Decorate a method that may be sync or async.

    This redecorates with the either @inlineCallbacks or @flatten_generator,
    depending on the `sync_attr`.
    """
    if callable(sync_attr):
        # If we don't get a sync attribute name, default to 'is_sync'.
        return maybe_async('is_sync')(sync_attr)

    def redecorate(func):
        @wraps(func)
        def wrapper(self, *args, **kw):
            if getattr(self, sync_attr):
                return flatten_generator(func)(self, *args, **kw)
            return inlineCallbacks(func)(self, *args, **kw)
        return wrapper

    return redecorate
