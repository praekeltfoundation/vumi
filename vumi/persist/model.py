# -*- test-case-name: vumi.persist.tests.test_model -*-

"""Base classes for Vumi persistence models."""


from vumi.persist.fields import Field, FieldDescriptor


class ModelMetaClass(type):
    def __new__(mcs, name, bases, dict):
        # set default bucket suffix
        if "bucket" not in dict:
            dict["bucket"] = name.lower()

        # locate Field instances
        fields, descriptors = {}, {}
        class_dicts = [dict] + [base.__dict__ for base in reversed(bases)]
        for cls_dict in class_dicts:
            for key, possible_field in cls_dict.items():
                if key == "dynamic_fields" or key in fields:
                    continue
                if isinstance(possible_field, FieldDescriptor):
                    possible_field = possible_field.field  # copy descriptors
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

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)

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


class ModelProxy(object):
    def __init__(self, manager, modelcls):
        self._manager = manager
        self._modelcls = modelcls

    def __call__(self, key, **data):
        return self._modelcls(self._manager, key, **data)

    def load(self, key):
        return self._modelcls.load(self._manager, key)
