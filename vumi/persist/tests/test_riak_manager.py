"""Tests for vumi.persist.riak_manager."""

from itertools import count

from twisted.internet.defer import returnValue

from vumi.persist.tests.test_txriak_manager import (
    CommonRiakManagerTests, DummyModel)
from vumi.persist.model import Manager
from vumi.tests.helpers import VumiTestCase, import_skip


class TestRiakManager(CommonRiakManagerTests, VumiTestCase):
    """Most tests are inherited from the CommonRiakManagerTests mixin."""

    def setUp(self):
        try:
            from vumi.persist.riak_manager import (
                RiakManager, flatten_generator)
        except ImportError, e:
            import_skip(e, 'riak')
        self.call_decorator = flatten_generator
        self.manager = RiakManager.from_config({'bucket_prefix': 'test.'})
        self.add_cleanup(self.manager.purge_all)
        self.manager.purge_all()

    def test_call_decorator(self):
        self.assertEqual(type(self.manager).call_decorator,
                         self.call_decorator)

    def test_flatten_generator(self):
        results = []
        counter = count()

        @self.call_decorator
        def f():
            for i in range(3):
                a = yield counter.next()
                results.append(a)

        ret = f()
        self.assertEqual(ret, None)
        self.assertEqual(results, list(range(3)))

    def test_flatter_generator_with_return_value(self):
        @self.call_decorator
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
        self.assertEqual(
            [model.get_data() for model in mr_results],
            expected_data)

    def test_transport_class_protocol_buffer(self):
        manager_class = type(self.manager)
        manager = manager_class.from_config({
            'transport_type': 'pbc',
            'bucket_prefix': 'test.',
        })
        self.assertEqual(manager.client.protocol, 'pbc')

    def test_transport_class_http(self):
        manager_class = type(self.manager)
        manager = manager_class.from_config({
            'transport_type': 'http',
            'bucket_prefix': 'test.',
        })
        self.assertEqual(manager.client.protocol, 'http')

    def test_transport_class_default(self):
        manager_class = type(self.manager)
        manager = manager_class.from_config({
            'bucket_prefix': 'test.',
        })
        self.assertEqual(manager.client.protocol, 'http')
