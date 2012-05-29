"""Tests for vumi.persist.txriak_manager."""

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.persist.txriak_manager import TxRiakManager
from vumi.persist.model import Manager


class DummyModel(object):

    bucket = "dummy_model"

    def __init__(self, manager, key, _riak_object=None):
        self.manager = manager
        self.key = key
        self._riak_object = _riak_object

    def set_riak(self, riak_object):
        self._riak_object = riak_object

    def get_data(self):
        return self._riak_object.get_data()

    def set_data(self, data):
        self._riak_object.set_data(data)

    def add_index(self, index_name, key):
        self._riak_object.add_index(index_name, key)


class CommonRiakManagerTests(object):
    """Common tests for Riak managers.

    Tests assume self.manager is set to a suitable Riak
    manager.
    """

    def mkdummy(self, key, data=None):
        dummy = DummyModel(self.manager, key)
        dummy.set_riak(self.manager.riak_object(dummy, key))
        if data is not None:
            dummy.set_data(data)
        return dummy

    def test_from_config(self):
        manager_cls = self.manager.__class__
        manager = manager_cls.from_config({'bucket_prefix': 'test.'})
        self.assertEqual(manager.__class__, manager_cls)

    def test_sub_manager(self):
        sub_manager = self.manager.sub_manager("foo.")
        self.assertEqual(sub_manager.client, self.manager.client)
        self.assertEqual(sub_manager.bucket_prefix, 'test.foo.')

    def test_riak_object(self):
        dummy = DummyModel(self.manager, "foo")
        riak_object = self.manager.riak_object(dummy, "foo")
        self.assertEqual(riak_object.get_data(), {})
        self.assertEqual(riak_object.get_content_type(), "application/json")
        self.assertEqual(riak_object.get_bucket().get_name(),
                         "test.dummy_model")
        self.assertEqual(riak_object.get_key(), "foo")

    @Manager.calls_manager
    def test_store_and_load(self):
        dummy1 = self.mkdummy("foo", {"a": 1})
        result1 = yield self.manager.store(dummy1)
        self.assertEqual(dummy1, result1)

        dummy2 = yield self.manager.load(DummyModel, "foo")
        self.assertEqual(dummy2.get_data(), {"a": 1})

    @Manager.calls_manager
    def test_delete(self):
        dummy1 = self.mkdummy("foo", {"a": 1})
        yield self.manager.store(dummy1)

        dummy2 = yield self.manager.load(DummyModel, "foo")
        yield self.manager.delete(dummy2)

        dummy3 = yield self.manager.load(DummyModel, "foo")
        self.assertEqual(dummy3, None)

    @Manager.calls_manager
    def test_load_missing(self):
        dummy = self.mkdummy("unknown")
        result = yield self.manager.load(DummyModel, dummy.key)
        self.assertEqual(result, None)

    @Manager.calls_manager
    def test_load_list(self):
        yield self.manager.store(self.mkdummy("foo", {"a": 0}))
        yield self.manager.store(self.mkdummy("bar", {"a": 1}))

        keys = ["foo", "bar", "unknown"]

        result = yield self.manager.load_list(DummyModel, keys)
        result_data = [r.get_data() if r is not None else None for r in result]
        result_data.sort(key=lambda d: d["a"] if d is not None else -1)
        self.assertEqual(result_data, [None, {"a": 0}, {"a": 1}])

    @Manager.calls_manager
    def test_run_riak_map_reduce(self):
        dummies = [self.mkdummy(str(i), {"a": i}) for i in range(4)]
        for dummy in dummies:
            dummy.add_index('test_index_bin', 'test_key')
            yield self.manager.store(dummy)

        mr = self.manager.riak_map_reduce()
        mr.index('test.dummy_model', 'test_index_bin', 'test_key')
        mr_results = []

        def mapper(manager, link):
            self.assertEqual(manager, self.manager)
            mr_results.append(link)
            dummy = self.mkdummy(link.get_key())
            return manager.load(DummyModel, dummy.key)

        results = yield self.manager.run_map_reduce(mr, mapper)
        results.sort(key=lambda d: d.key)
        expected_keys = [str(i) for i in range(4)]
        self.assertEqual([d.key for d in results], expected_keys)
        mr_results.sort(key=lambda l: l.get_key())
        self.assertEqual([l.get_key() for l in mr_results], expected_keys)

    @Manager.calls_manager
    def test_purge_all(self):
        dummy = self.mkdummy("foo", {"baz": 0})
        yield self.manager.store(dummy)
        yield self.manager.purge_all()
        result = yield self.manager.load(DummyModel, dummy.key)
        self.assertEqual(result, None)


class TestTxRiakManager(CommonRiakManagerTests, TestCase):
    @inlineCallbacks
    def setUp(self):
        self.manager = TxRiakManager.from_config({'bucket_prefix': 'test.'})
        yield self.manager.purge_all()

    @inlineCallbacks
    def tearDown(self):
        yield self.manager.purge_all()

    def test_call_decorator(self):
        self.assertEqual(TxRiakManager.call_decorator, inlineCallbacks)
