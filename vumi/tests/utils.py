# -*- test-case-name: vumi.tests.test_testutils -*-

import json, importlib
from collections import namedtuple
from contextlib import contextmanager

import txamqp
from txamqp.client import TwistedDelegate
from twisted.internet import defer

from vumi.utils import make_vumi_path_abs
from vumi.service import Worker, WorkerAMQClient

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
        self.mod = importlib.import_module(self.function.__module__) 
        setattr(self.mod, self.function.func_name, self)
        return self

    def __exit__(self, *exc_info):
        """Reset to whatever the function was originally when done"""
        setattr(self.mod, self.function.func_name, self.function)

    def __call__(self, *args, **kwargs):
        """Return the return value when called, store the args & kwargs
        for testing later, called is a counter and evaluates to True
        if ever called."""
        self.args = args
        self.kwargs = kwargs
        self.called +=1 
        self.history.append(self.HistoryItem(args, kwargs))
        return self.return_value

    def to_return(self, *args):
        """Specify the return value"""
        self.return_value = args if len(args) > 1 else list(args).pop()
        return self

def mocking(fn): return Mocking(fn)

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


_TUPLE_CACHE = {}
def fake_amq_message(dictionary, delivery_tag='delivery_tag'):
    Content = namedtuple('Content', ['body'])
    Message = namedtuple('Message', ['content','delivery_tag'])
    return Message(
        delivery_tag=delivery_tag, 
        content=Content(body=json.dumps(dictionary))
    )


class TestQueue(object):
    """
    A test queue that mimicks txAMQP's queue behaviour
    """
    def __init__(self, queue):
        self.queue = queue

    def push(self, item):
        self.queue.append(item)

    def get(self):
        d = defer.Deferred()
        if self.queue:
            d.callback(self.queue.pop())
        return d

class TestChannel(object):
    def __init__(self):
        self.ack_log = []
        self.publish_log = []

    def basic_ack(self, tag, multiple=False):
        self.ack_log.append((tag, multiple))

    def channel_close(self, *args, **kwargs):
        d = defer.Deferred()
        d.callback(True)
        return d

    def basic_publish(self, *args, **kwargs):
        self.publish_log.append(kwargs)
    
    def exchange_declare(self, *args, **kwargs):
        pass

    def queue_declare(self, *args, **kwargs):
        pass

    def queue_bind(self, *args, **kwargs):
        pass

    def basic_consume(self, *args, **kwargs):
        klass = namedtuple('Reply', ['consumer_tag'])
        return klass(consumer_tag=1)

    def close(self, *args, **kwargs):
        return True


class StubbedAMQClient(WorkerAMQClient):
    def __init__(self, queue):
        self._queue = queue
        self.global_options = {}

    def get_channel(self):
        return TestChannel()

    def queue(self, *args, **kwargs):
        return self._queue


class TestWorker(Worker):
    def __init__(self, queue, config=None):
        if config is None:
            config = {}
        self._queue = queue
        Worker.__init__(self, StubbedAMQClient(queue), config)



class TestAMQClient(WorkerAMQClient):
    def __init__(self, global_options=None):
        spec = txamqp.spec.load(make_vumi_path_abs("config/amqp-spec-0-8.xml"))
        WorkerAMQClient.__init__(self, TwistedDelegate(), '', spec)
        if global_options is not None:
            self.global_options = global_options

    def get_channel(self):
        return TestChannel()

    @defer.inlineCallbacks
    def queue(self, key):
        yield self.queueLock.acquire()
        try:
            try:
                q = self.queues[key]
            except KeyError:
                q = TestQueue([])
                self.queues[key] = q
        finally:
            self.queueLock.release()
        defer.returnValue(q)


def get_stubbed_worker(worker_class):
    pass

