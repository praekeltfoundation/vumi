# -*- test-case-name: vumi.tests.test_testutils -*-

import re
import json
from datetime import datetime, timedelta
from collections import namedtuple
from contextlib import contextmanager
import warnings
from functools import wraps

import pytz
from twisted.trial.unittest import SkipTest
from twisted.internet import defer, reactor
from twisted.web.resource import Resource
from twisted.web.server import Site
from twisted.internet.defer import DeferredQueue, inlineCallbacks
from twisted.python import log

from vumi.utils import vumi_resource_path, import_module, flatten_generator
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


def import_skip(exc, *expected):
    msg = exc.args[0]
    module = msg.split()[-1]
    if expected and (module not in expected):
        raise
    raise SkipTest("Failed to import '%s'." % (module,))


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


def FakeRedis():
    warnings.warn("Use of FakeRedis is deprecated. "
                  "Use persist.tests.fake_redis instead.",
                  category=DeprecationWarning)
    from vumi.persist import fake_redis
    return fake_redis.FakeRedis()


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


class PersistenceMixin(object):
    sync_persistence = False

    sync_or_async = staticmethod(maybe_async('sync_persistence'))

    def _persist_setUp(self):
        self._persist_riak_managers = []
        self._persist_redis_managers = []
        self._persist_config = {
            'redis_manager': {
                'FAKE_REDIS': 'yes',
                'key_prefix': type(self).__module__,
                },
            'riak_manager': {
                'bucket_prefix': type(self).__module__,
                },
            }

    def mk_config(self, config):
        return dict(self._persist_config, **config)

    @maybe_async('sync_persistence')
    def _persist_tearDown(self):
        for manager in self._persist_riak_managers:
            yield self._persist_purge_riak(manager)
        for manager in self._persist_redis_managers:
            yield self._persist_purge_redis(manager)

    def _persist_purge_riak(self, manager):
        "This is a separate method to allow easy overriding."
        return manager.purge_all()

    def _persist_purge_redis(self, manager):
        "This is a separate method to allow easy overriding."
        return manager.close_manager()

    def get_riak_manager(self, config=None):
        if config is None:
            config = self._persist_config['riak_manager'].copy()

        if self.sync_persistence:
            return self._get_sync_riak_manager(config)
        return self._get_async_riak_manager(config)

    def _get_async_riak_manager(self, config):
        from vumi.persist.txriak_manager import TxRiakManager
        riak_manager = TxRiakManager.from_config(config)
        self._persist_riak_managers.append(riak_manager)
        return riak_manager

    def _get_sync_riak_manager(self, config):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')

        riak_manager = RiakManager.from_config(config)
        self._persist_riak_managers.append(riak_manager)
        return riak_manager

    def get_redis_manager(self, config=None):
        if config is None:
            config = self._persist_config['redis_manager'].copy()

        if self.sync_persistence:
            return self._get_sync_redis_manager(config)
        return self._get_async_redis_manager(config)

    def _get_async_redis_manager(self, config):
        from vumi.persist.txredis_manager import TxRedisManager

        d = TxRedisManager.from_config(config)

        def add_to_self(redis_manager):
            self._persist_redis_managers.append(redis_manager)
            return redis_manager

        return d.addCallback(add_to_self)

    def _get_sync_redis_manager(self, config):
        try:
            from vumi.persist.redis_manager import RedisManager
        except ImportError, e:
            import_skip(e, 'redis')

        redis_manager = RedisManager.from_config(config)
        self._persist_redis_managers.append(redis_manager)
        return redis_manager
