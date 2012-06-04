"""Tests for vumi.persist.riak_manager."""

from itertools import count
import socket

from twisted.trial.unittest import TestCase, SkipTest
from twisted.internet.defer import returnValue

from vumi.persist.riak_manager import RiakManager, flatten_generator
from vumi.persist.tests.test_txriak_manager import (CommonRiakManagerTests,
                                                        DummyModel)
from vumi.persist.model import Manager


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

    @Manager.calls_manager
    def test_run_riak_map_reduce_and_fetch_results(self):
        dummies = [self.mkdummy(str(i), {"a": i}) for i in range(4)]
        for dummy in dummies:
            dummy.add_index('test_index_bin', 'test_key')
            yield self.manager.store(dummy)

        mr = self.manager.riak_map_reduce()
        mr.index('test.dummy_model', 'test_index_bin', 'test_key')
        mr.map(function='function(v) { return [[v.key, v.values[0]]] }')
        # We should be using the sync manager and fetch the objects
        # as part of a mapreduce call
        self.assertTrue(self.manager.fetch_objects)
        mr_results = []

        def mapper(manager, key_and_result_tuple):
            self.assertEqual(manager, self.manager)
            key, result = key_and_result_tuple
            model_instance = manager.load(DummyModel, key, result)
            mr_results.append(model_instance)
            return model_instance

        results = yield self.manager.run_map_reduce(mr, mapper)
        results.sort(key=lambda d: d.key)
        expected_keys = [str(i) for i in range(4)]
        expected_data = [{"a": i} for i in range(4)]
        self.assertEqual([d.key for d in results], expected_keys)
        mr_results.sort(key=lambda model_instance: model_instance.key)
        self.assertEqual([model.key for model in mr_results], expected_keys)
        self.assertEqual([model.get_data() for model in mr_results],
            expected_data)
