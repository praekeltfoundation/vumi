# -*- test-case-name: vumi.persist.tests.test_model -*-

"""Base classes for Vumi persistence models."""

from functools import wraps

from vumi.persist.fields import Field, FieldDescriptor, ValidationError


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
                if key in fields:
                    continue
                if isinstance(possible_field, FieldDescriptor):
                    possible_field = possible_field.field  # copy descriptors
                if isinstance(possible_field, Field):
                    descriptors[key] = possible_field.get_descriptor(key)
                    dict[key] = descriptors[key]
                    fields[key] = possible_field
        dict["field_descriptors"] = descriptors

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

    # TODO: maybe replace .backlinks with a class-level .query
    #       or .by_<index-name> method

    def __init__(self, manager, key, _riak_object=None, **field_values):
        self.manager = manager
        self.key = key
        if _riak_object is not None:
            self._riak_object = _riak_object
        else:
            self._riak_object = manager.riak_object(self, key)
            for field_name, descriptor in self.field_descriptors.iteritems():
                field = descriptor.field
                if not field.initializable:
                    continue
                field_value = field_values.pop(field_name, field.default)
                if callable(field_value):
                    field_value = field_value()
                descriptor.initialize(self, field_value)
        if field_values:
            raise ValidationError("Unexpected extra initial fields %r passed"
                                  " to model %s" % (field_values.keys(),
                                                    self.__class__))

    def __repr__(self):
        fields = self.field_descriptors.keys()
        fields.sort()
        items = ["%s=%r" % (field, getattr(self, field)) for field in fields]
        return "<%s key=%s %s>" % (self.__class__.__name__, self.key,
                                   " ".join(items))

    def save(self):
        """Save the object to Riak.

        :returns:
            A deferred that fires once the data is saved (or None if
            using a synchronous manager).
        """
        return self.manager.store(self)

    def delete(self):
        """Delete the object from Riak.

        :returns:
            A deferred that fires once the data is deleted (or None if
            using a synchronous manager).
        """
        return self.manager.delete(self)

    @classmethod
    def load(cls, manager, key):
        """Load an object from Riak.

        :returns:
            A deferred that fires with the new model object.
        """
        return manager.load(cls, key)

    @classmethod
    def search(cls, manager, return_keys=False, **kw):
        """Perform a solr search over this model.

        :returns:
            A list of model instances (or a list of keys if
            return_keys is set to True).
        """
        # TODO: build the queries more intelligently
        for k, value in kw.iteritems():
            value = unicode(value)
            value = value.replace('\\', '\\\\')
            value = value.replace("'", "\\'")
            kw[k] = value
        query = " AND ".join("%s:'%s'" % (k, v) for k, v in kw.iteritems())
        return manager.riak_search(cls, query, return_keys=return_keys)

    @classmethod
    def enable_search(cls, manager):
        """Enable solr indexing over for this model and manager."""
        return manager.riak_enable_search(cls)


class Manager(object):
    """A wrapper around a Riak client."""

    def __init__(self, client, bucket_prefix):
        self.client = client
        self.bucket_prefix = bucket_prefix

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)

    def sub_manager(self, sub_prefix):
        return self.__class__(self.client, self.bucket_prefix + sub_prefix)

    def bucket_name(self, modelcls_or_obj):
        return self.bucket_prefix + modelcls_or_obj.bucket

    @staticmethod
    def calls_manager(manager_attr):
        """Decorate a method that calls a manager.

        This redecorates with the `call_decorator` attribute on the Manager
        subclass used, which should be either @inlineCallbacks or
        @flatten_generator.
        """
        if callable(manager_attr):
            # If we don't get a manager attribute name, default to 'manager'.
            return Manager.calls_manager('manager')(manager_attr)

        def redecorate(func):
            @wraps(func)
            def wrapper(self, *args, **kw):
                manager = getattr(self, manager_attr)
                return manager.call_decorator(func)(self, *args, **kw)
            return wrapper

        return redecorate

    @classmethod
    def from_config(cls, config):
        """Construct a manager from a dictionary of options.

        :param dict config:
            Dictionary of options for the manager.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .from_config(...)")

    def riak_object(self, cls, key):
        """Construct an empty RiakObject for the given model class and key."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_object(...)")

    def store(self, modelobj):
        """Store the modelobj in Riak."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .store(...)")

    def delete(self, modelobj):
        """Delete the modelobj from Riak."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .delete(...)")

    def load(self, cls, key):
        """Load a model instance for the key from Riak.

        If the key doesn't exist, this method should return None
        instead of an instance of cls.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .store(...)")

    def load_list(self, cls, keys):
        """Load the model instances for a list of keys from Riak.

        If a key doesn't exist, that key should be replaced by a None
        (instead of an instance of cls) in the list returned.
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

    def riak_search(self, cls, query, return_keys=False):
        """Run a solr search over the bucket associated with cls and
        return the results as instances of cls (or as keys if return_keys is
        set to True)."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_search(...)")

    def riak_enable_search(self, cls):
        """Enable solr searching indexing for the bucket associated with
        cls."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_enable_search(...)")

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

    def search(self, **kw):
        return self._modelcls.search(self._manager, **kw)

    def enable_search(self):
        return self._modelcls.enable_search(self._manager)
