# -*- test-case-name: vumi.persist.tests.test_model -*-

"""Base class for Vumi persistence models."""

from twisted.internet.defer import inlineCallbacks

from vumi.persist.fields import Field


class ModelMetaClass(type):
    def __new__(mcs, name, bases, dict):
        # set default bucket suffix
        if "bucket" not in dict:
            dict["bucket"] = name.lower()

        # locate Field instances
        fields, descriptors = {}, {}
        for key, possible_field in dict.items():
            if key == "dynamic_fields":
                continue
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

    def __init__(self, manager, key, **field_values):
        self.key = key
        self.manager = manager
        self._riak_object = manager.riak_object(self)
        for field_name, field_value in field_values.iteritems():
            setattr(self, field_name, field_value)

    def save(self):
        """Save the object to Riak.

        :returns:
            A deferred that fires once the data is saved.
        """
        return self.manager.store(self)

    @classmethod
    def load(cls, manager, key):
        """Load an object from Riak.

        :returns:
            A deferred that fires with the new model object.
        """
        modelobj = cls(manager, key)
        return manager.load(modelobj)


class Manager(object):
    """A wrapper around a Riak client."""

    def __init__(self, client, bucket_prefix):
        self.client = client
        self.bucket_prefix = bucket_prefix

    @classmethod
    def from_config(cls, config):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        """
        raise NotImplementedError("Sub-classes of Manger should implement"
                                  " .from_config(...)")

    def riak_object(self, modelobj):
        """Construct an empty RiakObject for the given model instance."""
        raise NotImplementedError("Sub-classes of Manger should implement"
                                  " .riak_object(...)")

    def store(self, modelobj):
        """Store the modelobj in Riak."""
        raise NotImplementedError("Sub-classes of Manger should implement"
                                  " .store(...)")

    def load(self, modelobj):
        """Load the data for the modelobj from Riak."""
        raise NotImplementedError("Sub-classes of Manger should implement"
                                  " .store(...)")

    def purge_all(self):
        """Delete *ALL* keys in buckets whose names start buckets with
        this manager's bucket prefix.

        Use only in tests.
        """
        raise NotImplementedError("Sub-classes of Manger should implement"
                                  " .purge_all()")

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)


class TxRiakManager(Manager):
    """A wrapper around a txriak client."""

    @classmethod
    def from_config(cls, config):
        from txriak.riak import RiakClient
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, modelobj):
        from txriak.riak import RiakObject
        bucket_name = self.bucket_prefix + modelobj.bucket
        bucket = self.client.bucket(bucket_name)
        riak_object = RiakObject(self.client, bucket, modelobj.key)
        riak_object.set_data({})
        riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        d = modelobj._riak_object.store()
        d.addCallback(lambda result: modelobj)
        return d

    def load(self, modelobj):
        d = modelobj._riak_object.reload()
        d.addCallback(lambda result: modelobj)
        return d

    @inlineCallbacks
    def purge_all(self):
        buckets = yield self.client.list_buckets()
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                yield bucket.purge_keys()

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)


class RiakManager(Manager):
    """A wrapper around a txriak client."""

    @classmethod
    def from_config(cls, config):
        from riak import RiakClient
        bucket_prefix = config.pop('bucket_prefix')
        client = RiakClient(**config)
        return cls(client, bucket_prefix)

    def riak_object(self, modelobj):
        from riak import RiakObject
        bucket_name = self.bucket_prefix + modelobj.bucket
        bucket = self.client.bucket(bucket_name)
        riak_object = RiakObject(self.client, bucket, modelobj.key)
        riak_object.set_data({})
        riak_object.set_content_type("application/json")
        return riak_object

    def store(self, modelobj):
        modelobj._riak_object.store()
        return modelobj

    def load(self, modelobj):
        modelobj._riak_object.reload()
        return modelobj

    def purge_all(self):
        buckets = self.client.get_buckets()
        for bucket_name in buckets:
            if bucket_name.startswith(self.bucket_prefix):
                bucket = self.client.bucket(bucket_name)
                for key in bucket.get_keys():
                    obj = bucket.get(key)
                    obj.delete()

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
