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
    def index_lookup(cls, manager, field_name, value):
        """Find objects by index.

        :returns: :class:`VumiMapReduce` instance based on the index param.
        """
        return manager.mr_from_field(cls, field_name, value)

    @classmethod
    def search(cls, manager, **kw):
        """Search for instances of this model matching keys/values.

        :returns: :class:`VumiMapReduce` instance based on the search params.
        """
        # TODO: build the queries more intelligently
        for k, value in kw.iteritems():
            value = unicode(value)
            value = value.replace('\\', '\\\\')
            value = value.replace("'", "\\'")
            kw[k] = value
        query = " AND ".join("%s:'%s'" % (k, v) for k, v in kw.iteritems())
        return cls.raw_search(manager, query)

    @classmethod
    def raw_search(cls, manager, query):
        """
        Performs a raw riak search, does no inspection on the given query.

        :returns: :class:`VumiMapReduce` instance based on the search params.
        """
        return manager.mr_from_search(cls, query)

    @classmethod
    def load_from_keys(cls, manager, keys):
        """
        Load objects for the given list of keys.

        :returns:
            A list of model instances.
        """
        return manager.load_from_keys(cls, keys)

    @classmethod
    def enable_search(cls, manager):
        """Enable solr indexing over for this model and manager."""
        return manager.riak_enable_search(cls)


class VumiMapReduce(object):
    def __init__(self, mgr, riak_mapreduce_obj):
        self._manager = mgr
        self._riak_mapreduce_obj = riak_mapreduce_obj

    @classmethod
    def from_field(cls, mgr, model, field_name, start_value, end_value=None):
        descriptor = model.field_descriptors[field_name]
        if descriptor.index_name is None:
            raise ValueError("%s.%s is not indexed" % (
                    model.__name__, field_name))

        # The Riak client library does silly things under the hood.
        start_value = descriptor.field.to_riak(start_value)
        if start_value is None:
            start_value = ''
            # We still rely on this having the value "None" in places. :-(
            start_value = 'None'
        else:
            start_value = str(start_value)

        if end_value is not None:
            end_value = str(descriptor.field.to_riak(end_value))

        return cls.from_index(
            mgr, model, descriptor.index_name, start_value, end_value)

    @classmethod
    def from_index(cls, mgr, model, index_name, start_value, end_value=None):
        return cls(mgr, mgr.riak_map_reduce().index(
                mgr.bucket_name(model), index_name, start_value, end_value))

    @classmethod
    def from_search(cls, mgr, model, query):
        return cls(
            mgr, mgr.riak_map_reduce().search(mgr.bucket_name(model), query))

    @classmethod
    def from_keys(cls, mgr, model, keys):
        bucket_name = mgr.bucket_name(model)
        mr = mgr.riak_map_reduce()
        for key in keys:
            mr.add_bucket_key_data(bucket_name, key, None)
        return cls(mgr, mr)

    def filter_not_found(self):
        self._riak_mapreduce_obj.map(function="""
            function(v) {
                values = v.values.filter(function(val) {
                        return !val.metadata['X-Riak-Deleted']
                    })
                if (values) {
                    return [v.key];
                } else {
                    return [];
                }
            }""")
        self._riak_mapreduce_obj.filter_not_found()

    def get_count(self):
        self._riak_mapreduce_obj.reduce(
            function=["riak_kv_mapreduce", "reduce_count_inputs"])
        return self._manager.run_map_reduce(
            self._riak_mapreduce_obj, reducer_func=lambda mgr, obj: obj[0])

    def _results_to_keys(self, mgr, obj):
        if isinstance(obj, basestring):
            # Assume strings are keys.
            return obj
        else:
            # If we haven't been given a string, we probably have a RiakLink.
            return obj.get_key()

    def get_keys(self):
        return self._manager.run_map_reduce(
            self._riak_mapreduce_obj, self._results_to_keys)


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

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        """Run a map reduce instance and return the results mapped to
        objects by the map_function."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .run_map_reduce(...)")

    def mr_from_field(self, model, field_name, start_value, end_value=None):
        return VumiMapReduce.from_field(
            self, model, field_name, start_value, end_value)

    def mr_from_index(self, model, index_name, start_value, end_value=None):
        return VumiMapReduce.from_index(
            self, model, index_name, start_value, end_value)

    def mr_from_search(self, model, query):
        return VumiMapReduce.from_search(self, model, query)

    def mr_from_keys(self, model, keys):
        return VumiMapReduce.from_keys(self, model, keys)

    def load_from_keys(self, model, keys):
        """Load the model instances for a list of keys from Riak.

        If a key doesn't exist, no object will be returned for it.
        """
        assert len(keys) <= 100
        if not keys:
            return []
        mr = self.mr_from_keys(model, keys)
        mr._riak_mapreduce_obj.map(function="""
                function (v) {
                    return [[v.key, v.values[0]]]
                }
                """).filter_not_found()
        return self.run_map_reduce(
            mr._riak_mapreduce_obj, lambda mgr, obj: model.load(mgr, *obj))

    def raw_search(self, model, query):
        """Find objects matching the search query in the model's bucket."""
        # TODO: Replace and deprecate?
        return self.mr_from_search(model, query).get_keys()

    def raw_search_count(self, model, query):
        # TODO: Replace and deprecate?
        return self.mr_from_search(model, query).get_count()

    def riak_enable_search(self, model):
        """Enable search indexing for the model's bucket."""
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
        self.bucket = modelcls.bucket

    def __call__(self, key, **data):
        return self._modelcls(self._manager, key, **data)

    def load(self, key):
        return self._modelcls.load(self._manager, key)

    def index_lookup(self, field_name, value):
        return self._modelcls.index_lookup(self._manager, field_name, value)

    def search(self, **kw):
        return self._modelcls.search(self._manager, **kw)

    def raw_search(self, query):
        return self._modelcls.raw_search(self._manager, query)

    def load_from_keys(self, *args, **kw):
        return self._modelcls.load_from_keys(self._manager, *args, **kw)

    def enable_search(self):
        return self._modelcls.enable_search(self._manager)
