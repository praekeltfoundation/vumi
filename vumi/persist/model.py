# -*- test-case-name: vumi.persist.tests.test_model -*-

"""Base class for Vumi persistence models."""

from twisted.internet.defer import inlineCallbacks

from txriak.riak_object import RiakObject
from txriak.client import RiakClient

from vumi.persist.fields import Field


class ModelMetaClass(type):
    def __new__(mcs, name, bases, dict):
        # set default bucket suffix
        if "bucket" not in dict:
            dict["bucket"] = name.lower()

        # locate Field instances
        fields, descriptors = {}, {}
        for key, possible_field in dict.items():
            if isinstance(possible_field, Field):
                descriptors[key] = possible_field.get_descriptor(key)
                dict[key] = descriptors[key]
                fields[key] = possible_field
        dict["fields"] = fields

        cls = type.__new__(mcs, name, bases, dict)

        # inform field instances which classes they belong to
        for field_descriptor in descriptors.itervalues():
            field_descriptor.setup(cls)

        return cls


class Model(object):
    """A model is a description of an entity persisted in a data store."""

    __metaclass__ = ModelMetaClass

    bucket = None
    dynamic_fields = None

    def __init__(self, manager, key, **field_values):
        self.key = key
        self._manager = manager
        self._riak_object = manager.riak_object(self)
        for field_name, field_value in field_values.iteritems():
            setattr(self, field_name, field_value)

    def __getattr__(self, key):
        if self.dynamic_fields is None:
            raise AttributeError("%r has not attribute %r" % (self, key))
        return self._riak_object._data[key]

    # TODO: having __setattr__ requires all sorts of weirdness elsewhere
    ## def __setattr__(self, key, value):
    ##     if key in self.fields:
    ##         return object.__setattr__(self, key, value)
    ##     if self.dynamic_fields is None:
    ##         raise AttributeError("%r does not allow setting of %r"
    ##                              % (self, key))
    ##     self.dynamic_fields.validate(value)
    ##     self._data[key] = value

    def save(self):
        """Save the object to Riak.

        :returns:
            A deferred that fires once the data is saved.
        """
        return self._riak_object.store()

    @classmethod
    def load(cls, manager, key):
        """Load an object from Riak.

        :returns:
            A deferred that fires with the new model object.
        """
        modelobj = cls(manager, key)
        d = modelobj._riak_object.reload()
        d.addCallback(lambda _riak_object: modelobj)
        return d


class Manager(object):
    """A wrapper around a Riak client."""

    def __init__(self, client, bucket_prefix):
        self.client = client
        self.bucket_prefix = bucket_prefix

    @classmethod
    def from_config(cls, config):
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, modelobj):
        bucket_name = self.bucket_prefix + modelobj.bucket
        bucket = self.client.bucket(bucket_name)
        riak_object = RiakObject(self.client, bucket, modelobj.key)
        riak_object.set_data({})
        riak_object.set_content_type("application/json")
        return riak_object

    @inlineCallbacks
    def purge_all(self):
        """Delete *ALL* keys in buckets whose names start buckets with
        this manager's bucket prefix.

        Use only in tests.
        """
        buckets = yield self.client.list_buckets()
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                yield bucket.purge_keys()

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)


class ModelProxy(object):
    def __init__(self, manager, modelcls):
        self._manager = manager
        self._modelcls = modelcls

    def __call__(self, key, **data):
        return self._modelcls(self._manager, key, **data)

    def load(self, key):
        return self._modelcls.load(self._manager, key)


# TODO: Write an XML RPC server that takes a manager and a list of model
#       classes and provides access to those over XML RPC.
