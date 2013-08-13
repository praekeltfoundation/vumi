# -*- test-case-name: vumi.persist.tests.test_riak_manager -*-

"""A manager implementation on top of the riak Python package."""

import json

from riak import RiakClient, RiakObject, RiakMapReduce

from vumi.persist.model import Manager
from vumi.utils import flatten_generator


def mk_proxy_property(attr):
    def getter(self):
        return getattr(self._riak_object, attr)

    def setter(self, value):
        setattr(self._riak_object, attr, value)

    return property(getter, setter)


class RiakIndexWrapper(object):
    def __init__(self, entry):
        self.entry = entry

    def get_field(self):
        return self.entry[0]

    def get_value(self):
        return self.entry[1]


class RiakObjectWrapper(object):
    _riak_object = None

    def __init__(self, riak_object):
        self._riak_object = riak_object

    key = mk_proxy_property('key')
    data = mk_proxy_property('data')
    _data = mk_proxy_property('data')
    content_type = mk_proxy_property('content_type')
    indexes = mk_proxy_property('indexes')
    store = mk_proxy_property('store')
    reload = mk_proxy_property('reload')
    delete = mk_proxy_property('delete')
    add_index = mk_proxy_property('add_index')
    remove_index = mk_proxy_property('remove_index')

    def set_data(self, value):
        self._riak_object.data = value

    def get_data(self):
        return self._riak_object.data

    def get_bucket(self):
        return self._riak_object.bucket

    def set_indexes(self, value):
        self._riak_object.indexes = value

    def get_indexes(self):
        return [RiakIndexWrapper(entry) for entry in self._riak_object.indexes]

    def set_encoded_data(self, value):
        self._riak_object.encoded_data = value

    def get_content_type(self):
        return self._riak_object.content_type

    def __setattr__(self, name, value):
        # Make sure we catch attempts to set attributes we aren't proxying.
        if not hasattr(self, name):
            raise AttributeError(name)
        super(RiakObjectWrapper, self).__setattr__(name, value)


class RiakManager(Manager):
    """A persistence manager for the riak Python package."""

    call_decorator = staticmethod(flatten_generator)

    @classmethod
    def from_config(cls, config):
        config = config.copy()
        bucket_prefix = config.pop('bucket_prefix')
        load_bunch_size = config.pop('load_bunch_size',
                                     cls.DEFAULT_LOAD_BUNCH_SIZE)
        mapreduce_timeout = config.pop('mapreduce_timeout',
                                       cls.DEFAULT_MAPREDUCE_TIMEOUT)
        transport_type = config.pop('transport_type', 'http')
        protocol, port_arg = {
            'http': ('http', 'http_port'),
            'https': ('https', 'http_port'),
            'protocol_buffer': ('pbc', 'pb_port'),
        }.get(transport_type, ('http', 'http_port'))

        host = config.get('host', '127.0.0.1')
        port = config.get('port', 8098)
        client_id = config.get('client_id')

        client_args = {
            'protocol': protocol,
            'host': host,
            port_arg: port,
            'client_id': client_id,
        }
        if 'transport_options' in config:
            client_args['transport_options'] = config['transport_options']
        client = RiakClient(**client_args)
        # Some versions of the riak client library use simplejson by
        # preference, which breaks some of our unicode assumptions. This makes
        # sure we're using stdlib json which doesn't sometimes return
        # bytestrings instead of unicode.
        client.set_encoder('application/json', json.dumps)
        client.set_encoder('text/json', json.dumps)
        client.set_decoder('application/json', json.loads)
        client.set_decoder('text/json', json.loads)
        return cls(client, bucket_prefix, load_bunch_size=load_bunch_size,
                   mapreduce_timeout=mapreduce_timeout)

    def riak_object(self, modelcls, key, result=None):
        bucket = self.bucket_for_modelcls(modelcls)
        riak_object = RiakObjectWrapper(RiakObject(self.client, bucket, key))
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
            riak_object.content_type = metadata['content-type']
            riak_object.indexes = indexes
            riak_object.set_encoded_data(data)
        else:
            riak_object.data = {'$VERSION': modelcls.VERSION}
            riak_object.content_type = "application/json"
        return riak_object

    def store(self, modelobj):
        modelobj._riak_object.store()
        return modelobj

    def delete(self, modelobj):
        modelobj._riak_object.delete()

    def load(self, modelcls, key, result=None):
        riak_object = self.riak_object(modelcls, key, result)
        if not result:
            riak_object.reload()

        # Run migrators until we have the correct version of the data.
        while riak_object.get_data() is not None:
            data_version = riak_object.get_data().get('$VERSION', None)
            if data_version == modelcls.VERSION:
                return modelcls(self, key, _riak_object=riak_object)
            migrator = modelcls.MIGRATOR(modelcls, self, data_version)
            riak_object = migrator(riak_object).get_riak_object()
        return None

    def riak_map_reduce(self):
        return RiakMapReduce(self.client)

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        results = mapreduce.run(timeout=self.mapreduce_timeout)
        if mapper_func is not None:
            results = [mapper_func(self, row) for row in results]
        if reducer_func is not None:
            results = reducer_func(self, results)
        return results

    def index_keys(self, model, index_name, start_value, end_value=None):
        bucket = self.bucket_for_modelcls(model)
        # The IndexPage we get back from the new riak client doesn't fake
        # listiness quite well enough.
        return list(bucket.get_index(index_name, start_value, end_value))

    def riak_enable_search(self, modelcls):
        bucket_name = self.bucket_name(modelcls)
        bucket = self.client.bucket(bucket_name)
        return bucket.enable_search()

    def purge_all(self):
        buckets = self.client.get_buckets()
        for bucket in buckets:
            if bucket.name.startswith(self.bucket_prefix):
                for key in bucket.get_keys():
                    obj = bucket.get(key)
                    obj.delete()
                bucket.clear_properties()
