# -*- test-case-name: vumi.persist.tests.test_model -*-

"""Base class for Vumi persistence models."""

from twisted.internet.defer import inlineCallbacks

from txriak.riak_object import RiakObject
from txriak.client import RiakClient


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


class FieldDescriptor(object):
    """Property for getting and setting fields."""

    def __init__(self, key, field):
        self.key = key
        self.field = field

    def setup(self, cls):
        pass

    def validate(self, value):
        self.field.validate(value)

    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        return instance._data[self.key]

    def __set__(self, instance, value):
        self.validate(value)
        instance._data[self.key] = value


class Field(object):
    """Base class for model attributes / fields."""

    descriptor_class = FieldDescriptor

    def get_descriptor(self, key):
        return self.descriptor_class(key, self)

    def validate(self, value):
        """Check whether a value is valid for this field.

        Raise an exception if it isn't.
        """
        pass


class ValidationError(Exception):
    """Raised when a value assigned to a field is invalid."""


class Model(object):
    """A model is a description of an entity persisted in a data store."""

    __metaclass__ = ModelMetaClass

    bucket = None
    dynamic_fields = None

    def __init__(self, manager, key, **data):
        self._manager = manager
        self.key = key
        self._data = data

    def __getattr__(self, key):
        if self.dynamic_fields is None:
            raise AttributeError("%r has not attribute %r" % (self, key))
        return self._data[key]

    # TODO: having __setattr__ requires all sorts of weirdness elsewhere
    ## def __setattr__(self, key, value):
    ##     if key in self.fields:
    ##         return object.__setattr__(self, key, value)
    ##     if self.dynamic_fields is None:
    ##         raise AttributeError("%r does not allow setting of %r"
    ##                              % (self, key))
    ##     self.dynamic_fields.validate(value)
    ##     self._data[key] = value

    def to_riak(self, riak_object):
        """Populate a Riak object with the data from this model."""
        riak_object.set_data(self._data)

    def from_riak(self, riak_object):
        """Populate this object from a riak object."""
        self._data = riak_object.get_data()
        return self

    def save(self):
        """Save the object to the Riak data store.

        :returns:
            A deferred that fires once the data is saved.
        """
        return self._manager.save(self)

    @classmethod
    def load(cls, manager, key):
        return manager.load(cls, key)


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

    def save(self, modelobj):
        """Save an object in Riak.

        :returns:
            A deferred that fires once the data is saved.
        """
        bucket = self.bucket_prefix + modelobj.bucket
        riak_object = RiakObject(self.client, bucket, modelobj.key)
        modelobj.to_riak(riak_object)
        return riak_object.store()

    def load(self, modelcls, key):
        """Load an object from Riak.

        :returns:
            A deferred that fires with the new model object.
        """
        modelobj = modelcls(self, key)
        bucket = self.bucket_prefix + modelobj.bucket
        riak_object = RiakObject(self.client, bucket, modelobj.key)
        d = riak_object.reload()
        d.addCallback(modelobj.from_riak)
        return d

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


class Integer(Field):
    pass


class Unicode(Field):
    pass


class ForeignKeyDescriptor(object):
    def setup(self, cls):
        self.cls = cls
        self.othercls = self.field.othercls

    def validate(self, value):
        if not isinstance(value, self.othercls):
            raise ValidationError("Field %r of %r requires a %r" %
                                  (self.key, self.cls, self.othercls))

    def __get__(self, instance, owner):
        if instance is None:
            return self._field
        return None  # TODO: get this value from somewhere
        # return instance._data[self._key]

    def __set__(self, instance, value):
        self._field.validate(value)
        # TODO: write this value to a secondary index or something


class ForeignKey(Field):
    descriptor_class = ForeignKeyDescriptor

    def __init__(self, othercls, index=None):
        self._othercls = othercls
        self._index = index


# TODO: Write an XML RPC server that takes a manager and a list of model
#       classes and provides access to those over XML RPC.
