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
            self._riak_object = manager.riak_object(type(self), key)
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
    def load(cls, manager, key, result=None):
        """Load an object from Riak.

        :returns:
            A deferred that fires with the new model object.
        """
        return manager.load(cls, key, result=result)

    @classmethod
    def _from_index(cls, manager, **kw):
        kw_items = kw.items()
        if len(kw_items) != 1:
            raise ValueError("%s.by_index expects a key to search on." %
                             cls.__name__)
        key, value = kw_items[0]
        descriptor = cls.field_descriptors[key]
        if descriptor.index_name is None:
            raise ValueError("%s.%s is not indexed" % (cls.__name__, key))
        raw_value = descriptor.field.to_riak(value)

        mr = manager.riak_map_reduce()
        bucket = manager.bucket_name(cls)
        return mr.index(bucket, descriptor.index_name, unicode(raw_value))

    @classmethod
    def by_index(cls, manager, return_keys=False, **kw):
        """Find objects by index.

        :returns:
            A list of model instances (or a list of keys if
            return_keys is set to True).
        """
        mr = cls._from_index(manager, **kw)
        if return_keys:
            mapper = lambda manager, result: result.get_key()
        else:
            mapper = lambda manager, result: cls.load(manager,
                                                      result.get_key())
        return manager.run_map_reduce(mr, mapper)

    @classmethod
    def by_index_count(cls, manager, **kw):
        """Count objects by index.

        :returns:
            A count of items found.
        """
        mr = cls._from_index(manager, **kw)
        mr = mr.reduce(function=["riak_kv_mapreduce", "reduce_count_inputs"])
        return manager.run_map_reduce(mr)

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
        return cls.riak_search(manager, query, return_keys=return_keys)

    @classmethod
    def riak_search(cls, manager, query, return_keys=False):
        """
        Performs a raw riak search, does no inspection on the given query.

        :returns:
            A lit of model instances (or a list of keys if
            return_keys is set to True)
        """
        return manager.riak_search(cls, query, return_keys)

    @classmethod
    def riak_search_count(cls, manager, query):
        """
        Performs a raw riak search, does no inspection on the given query.

        :returns:
            A count of the results
        """
        return manager.riak_search_count(cls, query)

    @classmethod
    def enable_search(cls, manager):
        """Enable solr indexing over for this model and manager."""
        return manager.riak_enable_search(cls)


class Manager(object):
    """A wrapper around a Riak client."""

    def __init__(self, client, bucket_prefix):
        self.client = client
        self.bucket_prefix = bucket_prefix
        self._bucket_cache = {}

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)

    def sub_manager(self, sub_prefix):
        return self.__class__(self.client, self.bucket_prefix + sub_prefix)

    def bucket_name(self, modelcls_or_obj):
        return self.bucket_prefix + modelcls_or_obj.bucket

    def bucket_for_cls(self, cls):
        cls_id = id(cls)
        bucket = self._bucket_cache.get(cls_id)
        if bucket is None:
            bucket_name = self.bucket_name(cls)
            bucket = self.client.bucket(bucket_name)
            self._bucket_cache[cls_id] = bucket
        return bucket

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
        bucket_name = self.bucket_name(cls)
        mr = self.riak_map_reduce().search(bucket_name, query)
        if not return_keys:
            mr = mr.map(function="""
                function (v) {
                    return [[v.key, v.values[0]]]
                }
                """)

        def map_handler(manager, key_and_result):
            if return_keys:
                return key_and_result.get_key()
            else:
                key, result = key_and_result
                return cls.load(manager, key, result)

        return self.run_map_reduce(mr, map_handler)

    def riak_search_count(self, cls, query):
        bucket_name = self.bucket_name(cls)
        mr = self.riak_map_reduce().search(bucket_name, query)
        mr = mr.reduce(function=["riak_kv_mapreduce", "reduce_count_inputs"])
        return self.run_map_reduce(mr)

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

    def by_index(self, **kw):
        return self._modelcls.by_index(self._manager, **kw)

    def by_index_count(self, **kw):
        return self._modelcls.by_index_count(self._manager, **kw)

    def search(self, **kw):
        return self._modelcls.search(self._manager, **kw)

    def riak_search(self, *args, **kw):
        return self._modelcls.riak_search(self._manager, *args, **kw)

    def riak_search_count(self, *args, **kw):
        return self._modelcls.riak_search_count(self._manager, *args, **kw)

    def enable_search(self):
        return self._modelcls.enable_search(self._manager)
