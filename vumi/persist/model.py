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

        # add backlinks object
        dict["backlinks"] = BackLinks()

        cls = type.__new__(mcs, name, bases, dict)

        # inform field instances which classes they belong to
        for field_descriptor in descriptors.itervalues():
            field_descriptor.setup(cls)

        return cls


class BackLinks(object):
    """Object for holding reverse-key look-up functions for a Model class."""

    def __init__(self):
        self.functions = {}

    def declare_backlink(self, name, function):
        # TODO: add test for collision case
        if name in self.functions:
            raise RuntimeError("Backlink %r already registered" % (name,))
        self.functions[name] = function

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return BackLinkProxy(self, instance)


class BackLinkProxy(object):
    def __init__(self, backlinks, modelobj):
        self._backlinks = backlinks
        self._modelobj = modelobj

    def __getattr__(self, key):
        if key not in self._backlinks.functions:
            raise AttributeError("Not backlink function registered for %r"
                                 % (key,))

        def wrapped_backlink(*args, **kwargs):
            return self._backlinks.functions[key](self._modelobj, *args,
                                                  **kwargs)

        return wrapped_backlink


class Model(object):
    """A model is a description of an entity persisted in a data store."""

    __metaclass__ = ModelMetaClass

    bucket = None

    # TODO: implement default values
    # TODO: do a full check that all required values have
    #       a value at creation
    # TODO: maybe replace .backlinks with a class-level .query
    #       or .by_<index-name> method

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
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .from_config(...)")

    def riak_object(self, modelobj):
        """Construct an empty RiakObject for the given model instance."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_object(...)")

    def store(self, modelobj):
        """Store the modelobj in Riak."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .store(...)")

    def load(self, modelobj):
        """Load the data for the modelobj from Riak.

        If the key of the modelobj doesn't exist, this method should return
        None instead of the modelobj.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .store(...)")

    def riak_map_reduce(self):
        """Construct a RiakMapReduce object for this client."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_map_reduce(...)")

    def run_map_reduce(self, mapreduce, mapper_function):
        """Run a map reduce instance and return the results mapped to
        objects by the map_function."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_map_reduce(...)")

    def purge_all(self):
        """Delete *ALL* keys in buckets whose names start buckets with
        this manager's bucket prefix.

        Use only in tests.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .purge_all()")


class ModelProxy(object):
    def __init__(self, manager, modelcls):
        self._manager = manager
        self._modelcls = modelcls

    def __call__(self, key, **data):
        return self._modelcls(self._manager, key, **data)

    def load(self, key):
        return self._modelcls.load(self._manager, key)
