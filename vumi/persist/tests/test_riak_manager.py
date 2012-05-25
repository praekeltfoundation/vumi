"""Tests for vumi.persist.riak_manager."""

from itertools import count
import socket

from twisted.trial.unittest import TestCase, SkipTest
from twisted.internet.defer import returnValue

from vumi.persist.riak_manager import RiakManager, flatten_generator
from vumi.persist.tests.test_txriak_manager import CommonRiakManagerTests


class TestRiakManager(CommonRiakManagerTests, TestCase):
    """Most tests are inherited from the CommonRiakManagerTests mixin."""

    def setUp(self):
        self.manager = RiakManager.from_config({'bucket_prefix': 'test.'})
        try:
            self.manager.purge_all()
        except socket.error, e:
            raise SkipTest(e)

    def tearDown(self):
        self.manager.purge_all()

    def test_call_decorator(self):
        self.assertEqual(RiakManager.call_decorator, flatten_generator)

    def test_flatten_generator(self):
        results = []
        counter = count()

        @flatten_generator
        def f():
            for i in range(3):
                a = yield counter.next()
                results.append(a)

        ret = f()
        self.assertEqual(ret, None)
        self.assertEqual(results, list(range(3)))

    def test_flatter_generator_with_return_value(self):
        @flatten_generator
        def f():
            yield None
            returnValue("foo")

        ret = f()
        self.assertEqual(ret, "foo")
