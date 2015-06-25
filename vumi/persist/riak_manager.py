# -*- test-case-name: vumi.persist.tests.test_riak_manager -*-

"""A manager implementation on top of the riak Python package."""

from riak import RiakObject, RiakMapReduce, RiakError

from vumi.persist.model import Manager, VumiRiakError
from vumi.persist.riak_base import (
    VumiRiakClientBase, VumiIndexPageBase, VumiRiakBucketBase,
    VumiRiakObjectBase)
from vumi.utils import flatten_generator


class VumiRiakClient(VumiRiakClientBase):
    """
    Wrapper around a RiakClient to manage resources better.
    """


class VumiIndexPage(VumiIndexPageBase):
    """
    Wrapper around a page of index query results.

    Iterating over this object will return the results for the current page.
    """

    # Methods that touch the network.

    def next_page(self):
        """
        Fetch the next page of results.

        :returns:
            A new :class:`VumiIndexPage` object containing the next page of
            results.
        """
        if not self.has_next_page():
            return None
        try:
            result = self._index_page.next_page()
        except RiakError as e:
            raise VumiRiakError(e)
        return type(self)(result)


class VumiRiakBucket(VumiRiakBucketBase):
    """
    Wrapper around a RiakBucket to manage network access better.
    """

    # Methods that touch the network.

    def get_index(self, index_name, start_value, end_value=None,
                  return_terms=None):
        keys = self.get_index_page(
            index_name, start_value, end_value, return_terms=return_terms)
        return list(keys)

    def get_index_page(self, index_name, start_value, end_value=None,
                       return_terms=None, max_results=None, continuation=None):
        try:
            result = self._riak_bucket.get_index(
                index_name, start_value, end_value, return_terms=return_terms,
                max_results=max_results, continuation=continuation)
        except RiakError as e:
            raise VumiRiakError(e)
        return VumiIndexPage(result)


class VumiRiakObject(VumiRiakObjectBase):
    """
    Wrapper around a RiakObject to manage network access better.
    """

    def get_bucket(self):
        return VumiRiakBucket(self._riak_obj.bucket)

    # Methods that touch the network.

    def _call_and_wrap(self, func):
        """
        Call a function that touches the network and wrap the result in this
        class.
        """
        return type(self)(func())


class RiakManager(Manager):
    """A persistence manager for the riak Python package."""

    call_decorator = staticmethod(flatten_generator)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        load_bunch_size = config.pop(
            'load_bunch_size', cls.DEFAULT_LOAD_BUNCH_SIZE)
        mapreduce_timeout = config.pop(
            'mapreduce_timeout', cls.DEFAULT_MAPREDUCE_TIMEOUT)
        transport_type = config.pop('transport_type', 'http')
        store_versions = config.pop('store_versions', None)

        host = config.get('host', '127.0.0.1')
        port = config.get('port')
        prefix = config.get('prefix', 'riak')
        mapred_prefix = config.get('mapred_prefix', 'mapred')
        client_id = config.get('client_id')
        transport_options = config.get('transport_options', {})

        client_args = dict(
            host=host, prefix=prefix, mapred_prefix=mapred_prefix,
            protocol=transport_type, client_id=client_id,
            transport_options=transport_options)

        if port is not None:
            client_args['port'] = port

        client = VumiRiakClient(**client_args)
        return cls(
            client, bucket_prefix, load_bunch_size=load_bunch_size,
            mapreduce_timeout=mapreduce_timeout, store_versions=store_versions)

    def close_manager(self):
        if self._parent is None:
            # Only top-level managers may close the client.
            self.client.close()

    def _is_unclosed(self):
        # This returns `True` if the manager needs to be explicitly closed and
        # hasn't been closed yet. It should only be used in tests that ensure
        # client objects aren't leaked.
        if self._parent is not None:
            return False
        return not self.client._closed

    def riak_bucket(self, bucket_name):
        bucket = self.client.bucket(bucket_name)
        if bucket is not None:
            bucket = VumiRiakBucket(bucket)
        return bucket

    def riak_object(self, modelcls, key, result=None):
        bucket = self.bucket_for_modelcls(modelcls)._riak_bucket
        riak_object = VumiRiakObject(RiakObject(self.client, bucket, key))
        if result:
            metadata = result['metadata']
            indexes = metadata['index']
            if hasattr(indexes, 'items'):
                # TODO: I think this is a Riak bug. In some cases
                #       (maybe when there are no indexes?) the index
                #       comes back as a list, in others (maybe when
                #       there are indexes?) it comes back as a dict.
                indexes = indexes.items()
            data = result['data']
            riak_object.set_content_type(metadata['content-type'])
            riak_object.set_indexes(indexes)
            riak_object.set_encoded_data(data)
        else:
            riak_object.set_content_type("application/json")
            riak_object.set_data({'$VERSION': modelcls.VERSION})
        return riak_object

    def store(self, modelobj):
        riak_object = self._reverse_migrate_riak_object(modelobj)
        riak_object.store()
        return modelobj

    def delete(self, modelobj):
        modelobj._riak_object.delete()

    def load(self, modelcls, key, result=None):
        riak_object = self.riak_object(modelcls, key, result)
        if not result:
            riak_object.reload()
        return self._migrate_riak_object(modelcls, key, riak_object)

    def _load_multiple(self, modelcls, keys):
        objs = (self.load(modelcls, key) for key in keys)
        return [obj for obj in objs if obj is not None]

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        results = mapreduce.run(timeout=self.mapreduce_timeout)
        if mapper_func is not None:
            results = [mapper_func(self, row) for row in results]
        if reducer_func is not None:
            results = reducer_func(self, results)
        return results

    def _search_iteration(self, bucket, query, rows, start):
        results = bucket.search(query, rows=rows, start=start)
        return [doc["id"] for doc in results["docs"]]

    def real_search(self, modelcls, query, rows=None, start=None):
        rows = 1000 if rows is None else rows
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        if start is not None:
            return self._search_iteration(bucket, query, rows, start)
        keys = []
        new_keys = self._search_iteration(bucket, query, rows, 0)
        while new_keys:
            keys.extend(new_keys)
            new_keys = self._search_iteration(bucket, query, rows, len(keys))
        return keys

    def riak_enable_search(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return bucket.enable_search()

    def riak_search_enabled(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return bucket.search_enabled()

    def should_quote_index_values(self):
        return False

    def purge_all(self):
        self.client._purge_all(self.bucket_prefix)
