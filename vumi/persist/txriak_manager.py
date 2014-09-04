# -*- test-case-name: vumi.persist.tests.test_txriak_manager -*-

"""An async manager implementation on top of the riak Python package."""

from twisted.internet.threads import deferToThread
from twisted.internet.defer import (
    inlineCallbacks, returnValue, gatherResults, maybeDeferred)

from vumi.persist import riak_manager
from vumi.persist.model import Manager


class VumiTxRiakBucket(riak_manager.VumiRiakBucket):
    # Methods that touch the network.

    def get_index(self, index_name, start_value, end_value=None):
        return deferToThread(
            self._riak_bucket.get_index, index_name, start_value, end_value)


class VumiTxRiakObject(riak_manager.VumiRiakObject):
    def get_bucket(self):
        return VumiTxRiakBucket(self._riak_obj.bucket)

    # Methods that touch the network.

    def store(self):
        d = deferToThread(self._riak_obj.store)
        d.addCallback(type(self))
        return d

    def reload(self):
        d = deferToThread(self._riak_obj.reload)
        d.addCallback(type(self))
        return d

    def delete(self):
        d = deferToThread(self._riak_obj.delete)
        d.addCallback(type(self))
        return d


class TxRiakManager(Manager):
    """An async persistence manager for the riak Python package."""

    call_decorator = staticmethod(inlineCallbacks)

    @classmethod
    def from_config(cls, config):
        sync_manager = riak_manager.RiakManager.from_config(config)
        return cls(
            sync_manager, sync_manager.bucket_prefix,
            load_bunch_size=sync_manager.load_bunch_size,
            mapreduce_timeout=sync_manager.mapreduce_timeout)

    def riak_bucket(self, bucket_name):
        bucket = self.client.riak_bucket(bucket_name)
        if bucket is not None:
            bucket = VumiTxRiakBucket(bucket._riak_bucket)
        return bucket

    def riak_object(self, modelcls, key, result=None):
        riak_object = self.client.riak_object(modelcls, key, result)
        return VumiTxRiakObject(riak_object._riak_obj)

    @inlineCallbacks
    def store(self, modelobj):
        yield modelobj._riak_object.store()
        returnValue(modelobj)

    @inlineCallbacks
    def delete(self, modelobj):
        yield modelobj._riak_object.delete()

    @inlineCallbacks
    def load(self, modelcls, key, result=None):
        riak_object = self.riak_object(modelcls, key, result)
        if not result:
            yield riak_object.reload()
        was_migrated = False

        # Run migrators until we have the correct version of the data.
        while riak_object.get_data() is not None:
            data_version = riak_object.get_data().get('$VERSION', None)
            if data_version == modelcls.VERSION:
                obj = modelcls(self, key, _riak_object=riak_object)
                obj.was_migrated = was_migrated
                returnValue(obj)
            migrator = modelcls.MIGRATOR(modelcls, self, data_version)
            riak_object = migrator(riak_object).get_riak_object()
            was_migrated = True
        returnValue(None)

    def _load_multiple(self, modelcls, keys):
        d = gatherResults([self.load(modelcls, key) for key in keys])
        d.addCallback(lambda objs: [obj for obj in objs if obj is not None])
        return d

    def riak_map_reduce(self):
        mapreduce = self.client.riak_map_reduce()
        # Hack: We replace the two methods that hit the network with
        #       deferToThread wrappers to prevent accidental sync calls in
        #       other code.
        run = mapreduce.run
        stream = mapreduce.stream
        mapreduce.run = lambda *a, **kw: deferToThread(run, *a, **kw)
        mapreduce.stream = lambda *a, **kw: deferToThread(stream, *a, **kw)
        return mapreduce

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        def map_results(raw_results):
            deferreds = []
            for row in raw_results:
                deferreds.append(maybeDeferred(mapper_func, self, row))
            return gatherResults(deferreds)

        mapreduce_done = mapreduce.run(timeout=self.mapreduce_timeout)
        if mapper_func is not None:
            mapreduce_done.addCallback(map_results)
        if reducer_func is not None:
            mapreduce_done.addCallback(lambda r: reducer_func(self, r))
        return mapreduce_done

    def riak_enable_search(self, modelcls):
        return deferToThread(self.client.riak_enable_search, modelcls)

    def should_quote_index_values(self):
        return False

    def purge_all(self):
        return deferToThread(self.client.purge_all)
