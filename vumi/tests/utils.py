# -*- test-case-name: vumi.tests.test_testutils -*-

import re
import json
import fnmatch
from datetime import datetime, timedelta
from collections import namedtuple
from contextlib import contextmanager

import pytz
from twisted.internet import defer, reactor
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.internet.defer import DeferredQueue, inlineCallbacks
from twisted.python import log

from vumi.utils import vumi_resource_path, import_module
from vumi.service import get_spec, Worker, WorkerCreator
from vumi.tests.fake_amqp import FakeAMQClient


def setup_django_test_database():
    from django.test.simple import DjangoTestSuiteRunner
    from django.test.utils import setup_test_environment
    from south.management.commands import patch_for_test_db_setup
    runner = DjangoTestSuiteRunner(verbosity=0, failfast=False)
    patch_for_test_db_setup()
    setup_test_environment()
    return runner, runner.setup_databases()


def teardown_django_test_database(runner, config):
    from django.test.utils import teardown_test_environment
    runner.teardown_databases(config)
    teardown_test_environment()


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


class Mocking(object):

    class HistoryItem(object):
        def __init__(self, args, kwargs):
            self.args = args
            self.kwargs = kwargs

    def __init__(self, function):
        """Mock a function"""
        self.function = function
        self.called = 0
        self.history = []
        self.return_value = None

    def __enter__(self):
        """Overwrite whatever module the function is part of"""
        self.mod = import_module(self.function.__module__)
        setattr(self.mod, self.function.__name__, self)
        return self

    def __exit__(self, *exc_info):
        """Reset to whatever the function was originally when done"""
        setattr(self.mod, self.function.__name__, self.function)

    def __call__(self, *args, **kwargs):
        """Return the return value when called, store the args & kwargs
        for testing later, called is a counter and evaluates to True
        if ever called."""
        self.args = args
        self.kwargs = kwargs
        self.called += 1
        self.history.append(self.HistoryItem(args, kwargs))
        return self.return_value

    def to_return(self, *args):
        """Specify the return value"""
        self.return_value = args if len(args) > 1 else list(args).pop()
        return self


def mocking(fn):
    return Mocking(fn)


class TestPublisher(object):
    """
    A test publisher that caches outbound messages in an internal queue
    for testing, instead of publishing over AMQP.

    Useful for testing consumers
    """
    def __init__(self):
        self.queue = []

    @contextmanager
    def transaction(self):
        yield

    def publish_message(self, message, **kwargs):
        self.queue.append((message, kwargs))


def fake_amq_message(dictionary, delivery_tag='delivery_tag'):
    Content = namedtuple('Content', ['body'])
    Message = namedtuple('Message', ['content', 'delivery_tag'])
    return Message(delivery_tag=delivery_tag,
                   content=Content(body=json.dumps(dictionary)))


class TestQueue(object):
    """
    A test queue that mimicks txAMQP's queue behaviour (DEPRECATED)
    """
    def __init__(self, queue, fake_broker=None):
        self.queue = queue
        self.fake_broker = fake_broker
        # TODO: Hook this up.

    def push(self, item):
        self.queue.append(item)

    def get(self):
        d = defer.Deferred()
        if self.queue:
            d.callback(self.queue.pop())
        return d


class TestChannel(object):
    "(DEPRECATED)"

    def __init__(self, channel_id=None, fake_broker=None):
        self.channel_id = channel_id
        self.fake_broker = fake_broker
        self.ack_log = []
        self.publish_log = []
        self.publish_message_log = self.publish_log  # TODO: Nuke

    def basic_ack(self, tag, multiple=False):
        self.ack_log.append((tag, multiple))

    def channel_close(self, *args, **kwargs):
        return defer.succeed(None)

    def channel_open(self, *args, **kwargs):
        return defer.succeed(None)

    def basic_publish(self, *args, **kwargs):
        self.publish_log.append(kwargs)
        if self.fake_broker:
            self.fake_broker.publish(**kwargs)

    def basic_qos(self, *args, **kwargs):
        return defer.succeed(None)

    def exchange_declare(self, *args, **kwargs):
        if self.fake_broker:
            self.fake_broker.exchange_declare(*args, **kwargs)

    def queue_declare(self, *args, **kwargs):
        if self.fake_broker:
            self.fake_broker.queue_declare(*args, **kwargs)

    def queue_bind(self, *args, **kwargs):
        if self.fake_broker:
            self.fake_broker.queue_bind(*args, **kwargs)

    def basic_consume(self, queue, **kwargs):
        return namedtuple('Reply', ['consumer_tag'])(consumer_tag=queue)

    def close(self, *args, **kwargs):
        return True


def get_stubbed_worker(worker_class, config=None, broker=None):
    spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
    amq_client = FakeAMQClient(spec, {}, broker)
    worker = worker_class({}, config)
    worker._amqp_client = amq_client
    return worker


class StubbedWorkerCreator(WorkerCreator):
    broker = None

    def _connect(self, worker, timeout, bindAddress):
        spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
        amq_client = FakeAMQClient(spec, self.options, self.broker)
        self.broker = amq_client.broker  # So we use the same broker for all.
        reactor.callLater(0, worker._amqp_connected, amq_client)


def get_stubbed_channel(broker=None, id=0):
    spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
    amq_client = FakeAMQClient(spec, {}, broker)
    return amq_client.channel(id)


class TestResourceWorker(Worker):
    port = 9999
    _resources = ()

    def set_resources(self, resources):
        self._resources = resources

    def startWorker(self):
        resources = [(cls(*args), path) for path, cls, args in self._resources]
        self.resources = self.start_web_resources(resources, self.port)
        return defer.succeed(None)

    def stopWorker(self):
        if self.resources:
            self.resources.stopListening()


class FakeRedis(object):
    """In process and memory implementation of redis-like data store.

    It's intended to match the Python redis module API closely so that
    it can be used in place of the redis module when testing.

    Known limitations:

    * Exceptions raised are not guaranteed to match the exception
      types raised by the real Python redis module.
    """

    def __init__(self):
        self._data = {}
        self._expiries = {}

    def teardown(self):
        self._clean_up_expires()

    def _clean_up_expires(self):
        for key in self._expiries.keys():
            delayed = self._expiries.pop(key)
            if not delayed.cancelled:
                delayed.cancel()

    # Global operations

    def exists(self, key):
        return key in self._data

    def keys(self, pattern='*'):
        return fnmatch.filter(self._data.keys(), pattern)

    def flushdb(self):
        self._data = {}

    # String operations

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value):
        value = str(value)  # set() sets string value
        self._data[key] = value

    def delete(self, key):
        existed = self.exists(key)
        self._data.pop(key, None)
        return existed

    # Integer operations

    # The python redis lib combines incr & incrby into incr(key, increment=1)
    def incr(self, key, increment=1):
        old_value = self._data.get(key)
        if old_value is None:
            old_value = 0
        new_value = int(old_value) + increment
        self.set(key, new_value)
        return new_value

    # Hash operations

    def hset(self, key, field, value):
        mapping = self._data.setdefault(key, {})
        new_field = field not in mapping
        mapping[field] = unicode(value)
        return int(new_field)

    def hget(self, key, field):
        return self._data.get(key, {}).get(field)

    def hdel(self, key, *fields):
        mapping = self._data.get(key)
        if mapping is None:
            return 0
        deleted = 0
        for field in fields:
            if field in mapping:
                del mapping[field]
                deleted += 1
        return deleted

    def hmset(self, key, mapping):
        hval = self._data.setdefault(key, {})
        hval.update(dict([(key, unicode(value))
            for key, value in mapping.items()]))

    def hgetall(self, key):
        return self._data.get(key, {}).copy()

    def hlen(self, key):
        return len(self._data.get(key, {}))

    def hvals(self, key):
        return self._data.get(key, {}).values()

    def hincrby(self, key, field, amount=1):
        value = self._data.get(key, {}).get(field, "0")
        # the int(str(..)) coerces amount to an int but rejects floats
        value = int(value) + int(str(amount))
        self._data.setdefault(key, {})[field] = str(value)
        return value

    def hexists(self, key, field):
        return int(field in self._data.get(key, {}))

    # Set operations

    def sadd(self, key, *values):
        sval = self._data.setdefault(key, set())
        sval.update(map(unicode, values))

    def smembers(self, key):
        return self._data.get(key, set())

    def spop(self, key):
        sval = self._data.get(key, set())
        if not sval:
            return None
        return sval.pop()

    def srem(self, key, value):
        sval = self._data.get(key, set())
        if value in sval:
            sval.remove(value)
            return 1
        return 0

    def scard(self, key):
        return len(self._data.get(key, set()))

    def smove(self, src, dst, value):
        result = self.srem(src, value)
        if result:
            self.sadd(dst, value)
        return result

    def sunion(self, key, *args):
        union = set()
        for rkey in (key,) + args:
            union.update(self._data.get(rkey, set()))
        return union

    def sismember(self, key, value):
        sval = self._data.get(key, set())
        return value in sval

    # Sorted set operations

    def zadd(self, key, **valscores):
        zval = self._data.setdefault(key, [])
        new_zval = [val for val in zval if val[1] not in valscores]
        for value, score in valscores.items():
            new_zval.append((score, value))
        new_zval.sort()
        self._data[key] = new_zval

    def zrem(self, key, value):
        zval = self._data.setdefault(key, [])
        new_zval = [val for val in zval if val[1] != value]
        self._data[key] = new_zval

    def zcard(self, key):
        return len(self._data.get(key, []))

    def zrange(self, key, start, stop, desc=False, withscores=False,
                score_cast_func=float):
        zval = self._data.get(key, [])
        stop += 1  # redis start/stop are element indexes
        if stop == 0:
            stop = None
        results = sorted(zval[start:stop],
                    key=lambda (score, _): score_cast_func(score))
        if desc:
            results.reverse()
        if withscores:
            return results
        else:
            return [v for k, v in results]

    # List operations
    def llen(self, key):
        return len(self._data.get(key, []))

    def lpop(self, key):
        if self.llen(key):
            return self._data[key].pop(0)

    def lpush(self, key, obj):
        self._data.setdefault(key, []).insert(0, obj)

    def rpush(self, key, obj):
        self._data.setdefault(key, []).append(obj)
        return self.llen(key) - 1

    def lrange(self, key, start, end):
        lval = self._data.get(key, [])
        if end >= 0 or end < -1:
            end += 1
        else:
            end = None
        return lval[start:end]

    def lrem(self, key, value, num=0):
        removed = [0]

        def keep(v):
            if v == value and (num == 0 or removed[0] < abs(num)):
                removed[0] += 1
                return False
            return True

        lval = self._data.get(key, [])
        if num >= 0:
            lval = [v for v in lval if keep(v)]
        else:
            lval.reverse()
            lval = [v for v in lval if keep(v)]
            lval.reverse()
        self._data[key] = lval
        return removed[0]

    # Expiry operations

    def expire(self, key, seconds):
        self.persist(key)
        delayed = reactor.callLater(seconds, self.delete, key)
        self._expiries[key] = delayed

    def persist(self, key):
        delayed = self._expiries.get(key)
        if delayed is not None and not delayed.cancelled:
            delayed.cancel()


class LogCatcher(object):
    """Gather logs."""

    def __init__(self):
        self.logs = []

    @property
    def errors(self):
        return [ev for ev in self.logs if ev["isError"]]

    def _gather_logs(self, event_dict):
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


class MockHttpServer(object):

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
        site_factory = Site(root)
        self._webserver = yield reactor.listenTCP(0, site_factory)
        self.addr = self._webserver.getHost()
        self.url = "http://%s:%s/" % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def stop(self):
        yield self._webserver.loseConnection()
