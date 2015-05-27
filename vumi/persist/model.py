# -*- test-case-name: vumi.persist.tests.test_model -*-

"""Base classes for Vumi persistence models."""

from functools import wraps
import urllib

from vumi.errors import VumiError
from vumi.persist.fields import Field, FieldDescriptor, ValidationError


class ModelMigrationError(VumiError):
    pass


class VumiRiakError(VumiError):
    pass


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
            raise AttributeError(
                "No backlink function registered for %r" % (key,))

        def wrapped_backlink(*args, **kwargs):
            return self._backlinks.functions[key](self._modelobj, *args,
                                                  **kwargs)

        return wrapped_backlink


class ModelMigrator(object):
    """
    Migration handler for old Model versions.

    Subclasses of this should implement ``migrate_from_<version>()`` methods
    for each previous version of the model being migrated. This method will
    be called with a :class:`MigrationData` instance and must return a
    :class:`MigrationData` instance. (This will likely be the same instance,
    but may be different.)

    The ``migrate_from_<version>()`` is allowed to do whatever other operations
    may be required (for example, modifying related objects). However, care
    should be taken to avoid lenthly delays, race conditions, etc.

    There is a special-case ``migrate_from_unversioned()`` method that is
    called for objects that do not contain a model version.

    In order to facilitate different processes using different model versions,
    reverse migrations are also supported. These are similar to forward
    migrations, except they are applied at save time (rather than load time)
    and methods are named ``reverse_from_<version>()``.
    """
    def __init__(self, model_class, manager, data_version, reverse=False):
        self.model_class = model_class
        self.manager = manager
        self.data_version = data_version
        self.reverse = reverse
        prefix = "reverse" if reverse else "migrate"
        if data_version is not None:
            migration_method_name = '%s_from_%s' % (prefix, str(data_version))
        else:
            migration_method_name = '%s_from_unversioned' % (prefix,)
        self.migration_method = getattr(self, migration_method_name, None)

    def __call__(self, riak_object):
        if self.migration_method is None:
            prefix = "reverse " if self.reverse else ""
            raise ModelMigrationError(
                'No %smigrators defined for %s version %s' % (
                    prefix, self.model_class.__name__, self.data_version))
        return self.migration_method(MigrationData(riak_object))


class MigrationData(object):
    def __init__(self, riak_object):
        self.riak_object = riak_object
        self.old_data = riak_object.get_data()
        self.new_data = {}
        self.old_index = {}
        self.new_index = {}
        for name, value in riak_object.get_indexes():
            self.old_index.setdefault(name, []).append(value)

    def get_riak_object(self):
        self.riak_object.set_data(self.new_data)
        # We need to explicitly remove old indexes before adding new ones.
        for field in self.old_index:
            self.riak_object.remove_index(field)
        for field, values in self.new_index.iteritems():
            for value in values:
                self.riak_object.add_index(field, value)
        return self.riak_object

    def copy_values(self, *fields):
        """Copy field values from old data to new data."""
        for field in fields:
            self.new_data[field] = self.old_data[field]

    def copy_indexes(self, *indexes):
        """Copy indexes from old data to new data."""
        for index in indexes:
            self.new_index[index] = self.old_index.get(index, [])[:]

    def copy_dynamic_values(self, *dynamic_prefixes):
        """Copy dynamic field values from old data to new data."""
        for prefix in dynamic_prefixes:
            for key in self.old_data:
                if key.startswith(prefix):
                    self.new_data[key] = self.old_data[key]

    def add_index(self, index, value):
        """Add a new index value to new data."""
        if index is None:
            index = ''
        else:
            index = str(index)
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        self.new_index.setdefault(index, []).append(value)

    def clear_index(self, index):
        """Remove all values for a given index from new data."""
        del self.new_index[index]

    def set_value(self, field, value, index=None, index_value=None):
        """Set the value (and optionally the index) for a field.

        Indexes are usually set by :class:`FieldDescriptor` objects. Since we
        don't have those here, we need to explicitly set the index values for
        fields that are indexed.
        """
        self.new_data[field] = value
        if index is not None:
            if index_value is None:
                index_value = value
            if index_value is not None:
                self.add_index(index, index_value)


class Model(object):
    """A model is a description of an entity persisted in a data store."""

    __metaclass__ = ModelMetaClass

    VERSION = None
    MIGRATOR = ModelMigrator

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
        self.clean()
        self.was_migrated = False

    def __repr__(self):
        str_items = ["%s=%r" % item for item
                     in sorted(self.get_data().items())]
        return "<%s %s>" % (self.__class__.__name__, " ".join(str_items))

    def clean(self):
        for field_name, descriptor in self.field_descriptors.iteritems():
            descriptor.clean(self)

    def get_data(self):
        """
        Returns a dictionary with for all known field names & values.
        Useful for when needing to represent a model instance as a dictionary.

        :returns:
            A dict of all values, including the key.
        """
        data = self._riak_object.get_data()
        data.update({
            'key': self.key,
        })
        return data

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
    def load_all_bunches(cls, manager, keys):
        """Load batches of objects for the given list of keys.

        :returns:
            An iterator over (possibly deferred) lists of model instances.
        """
        return manager.load_all_bunches(cls, keys)

    @classmethod
    def all_keys(cls, manager):
        """Return all keys in this model's bucket.

        Uses Riak's special `$bucket` index. Beware of tombstones (i.e.
        the keys returned might have been deleted from Riak in the near past).

        :returns:
            List of keys from this model's bucket.
        """
        return manager.index_keys(
            cls, '$bucket', manager.bucket_name(cls), None)

    @classmethod
    def index_keys(cls, manager, field_name, value, end_value=None,
                   return_terms=None):
        """Find object keys by index.

        :param manager:
            A :class:`Manager` object.

        :param str field_name:
            The name of the field to get the index from. The index type
            (integer or binary) is determined by the field and this may affect
            the behaviour of range queries.

        :param value:
            The index value to look up. This is processed by the field in
            question to get the actual value to send to Riak. If ``end_value``
            is provided, ``value`` is used as the start of a range query,
            otherwise an exact match is performed.

        :param end_value:
            The index value to use as the end of a range query. This is
            processed by the field in question to get the actual value to send
            to Riak. If provided, a range query is performed.

        :param bool return_terms:
            If ``True``, the raw index values will be returned along with the
            object keys in a ``(term, key)`` tuple. These raw values are not
            processed by the field and may therefore be different from the
            expected field values.

        :returns:
            List of keys matching the index param. If ``return_terms`` is
            ``True``, a list of ``(term, key)`` tuples will be returned
            instead.
        """
        index_name, start_value, end_value = index_vals_for_field(
            cls, field_name, value, end_value)
        return manager.index_keys(
            cls, index_name, start_value, end_value, return_terms=return_terms)

    @classmethod
    def all_keys_page(cls, manager, max_results=None, continuation=None):
        """Return all keys in this model's bucket.

        Uses Riak's special `$bucket` index. Beware of tombstones (i.e.
        the keys returned might have been deleted from Riak in the near past).

        :param int max_results:
            The maximum number of results to return per page. If ``None``,
            pagination will disables and a single page containing all results
            will be returned.

        :param continuation:
            An opaque continuation token indicating which page of results to
            fetch. The index page object returned from this method has a
            ``continuation`` attribute that contains this value. If ``None``,
            the first page of results will be returned.

        :returns:
            :class:`VumiIndexPage` or :class:`VumiTxIndexPage` object
            containing all keys from this model's bucket.
        """
        return manager.index_keys_page(
            cls, '$bucket', manager.bucket_name(cls), None,
            max_results=max_results, continuation=continuation)

    @classmethod
    def index_keys_page(cls, manager, field_name, value, end_value=None,
                        return_terms=None, max_results=None,
                        continuation=None):
        """Find object keys by index, using pagination.

        :param manager:
            A :class:`Manager` object.

        :param str field_name:
            The name of the field to get the index from. The index type
            (integer or binary) is determined by the field and this may affect
            the behaviour of range queries.

        :param value:
            The index value to look up. This is processed by the field in
            question to get the actual value to send to Riak. If ``end_value``
            is provided, ``value`` is used as the start of a range query,
            otherwise an exact match is performed.

        :param end_value:
            The index value to use as the end of a range query. This is
            processed by the field in question to get the actual value to send
            to Riak. If provided, a range query is performed.

        :param bool return_terms:
            If ``True``, the raw index values will be returned along with the
            object keys in a ``(term, key)`` tuple. These raw values are not
            processed by the field and may therefore be different from the
            expected field values.

        :param int max_results:
            The maximum number of results to return per page. If ``None``,
            pagination will disables and a single page containing all results
            will be returned.

        :param continuation:
            An opaque continuation token indicating which page of results to
            fetch. The index page object returned from this method has a
            ``continuation`` attribute that contains this value. If ``None``,
            the first page of results will be returned.

        :returns:
            :class:`VumiIndexPage` or :class:`VumiTxIndexPage` object
            containing results. If ``return_terms`` is ``True``, the object
            returned will contain ``(term, key)`` tuples instead of keys.
        """
        index_name, start_value, end_value = index_vals_for_field(
            cls, field_name, value, end_value)
        return manager.index_keys_page(
            cls, index_name, start_value, end_value, return_terms=return_terms,
            max_results=max_results, continuation=continuation)

    @classmethod
    def index_lookup(cls, manager, field_name, value):
        """Find objects by index.

        :returns: :class:`VumiMapReduce` instance based on the index param.
        """
        return manager.mr_from_field(cls, field_name, value)

    @classmethod
    def index_match(cls, manager, query, field_name, value):
        """
        Finds objects in the index that match the regex patterns in query

        :param list query:
            A list of dictionaries with query information. Each dictionary
            should have the follow structure:

            {
                "key": "the key to use to lookup the value in the JSON doc",
                "pattern": "the regex to match the value with",
                "flags": "the flags to set on the RegExp object",
            }

        :returns: class:`VumiMapReduce` instance based on the index param
                    with and a map phase for matching against the query.
        """
        return manager.mr_from_field_match(cls, query, field_name, value)

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
    def real_search(cls, manager, query, rows=None, start=None):
        """
        Performs a real riak search, does no inspection on the given query.

        :returns: list of keys.
        """
        return manager.real_search(cls, query, rows=rows, start=start)

    @classmethod
    def enable_search(cls, manager):
        """Enable solr indexing over for this model and manager."""
        return manager.riak_enable_search(cls)


def index_vals_for_field(model, field_name, start_value, end_value):
    descriptor = model.field_descriptors[field_name]
    if descriptor.index_name is None:
        raise ValueError("%s.%s is not indexed" % (
            model.__name__, field_name))

    # The Riak client library does silly things under the hood.
    start_value = descriptor.field.to_riak(start_value)
    if start_value is None:
        # FIXME: We should be raising an exception here, but we still rely on
        # this having the value "None" in places. :-(
        start_value = 'None'
    else:
        start_value = str(start_value)

    if end_value is not None:
        end_value = str(descriptor.field.to_riak(end_value))
    return descriptor.index_name, start_value, end_value


class VumiMapReduceError(Exception):
    pass


class VumiMapReduce(object):
    def __init__(self, mgr, riak_mapreduce_obj):
        self._has_run = False
        self._manager = mgr
        self._riak_mapreduce_obj = riak_mapreduce_obj

    @classmethod
    def from_field(cls, mgr, model, field_name, start_value, end_value=None):
        index_name, sv, ev = index_vals_for_field(
            model, field_name, start_value, end_value)
        return cls.from_index(mgr, model, index_name, sv, ev)

    @classmethod
    def from_index(cls, mgr, model, index_name, start_value, end_value=None):
        return cls(mgr, mgr.riak_map_reduce().index(
            mgr.bucket_name(model), index_name, start_value, end_value))

    @classmethod
    def from_search(cls, mgr, model, query):
        return cls(
            mgr, mgr.riak_map_reduce().search(mgr.bucket_name(model), query))

    @classmethod
    def from_field_match(cls, mgr, model, query, field_name, start_value,
                         end_value=None):
        index_name, sv, ev = index_vals_for_field(
            model, field_name, start_value, end_value)
        return cls.from_index_match(mgr, model, query, index_name, sv, ev)

    @classmethod
    def from_index_match(cls, mgr, model, query, index_name, start_value,
                         end_value=None):
        """
        Do a regex OR search across the keys found in a secondary index.

        :param Manager mgr:
            The manager to use.
        :param Model model:
            The model to use.
        :param query:
            A list of dictionaries to use to search with. The dictionary is
            in the following format:

            {
                "key": "the key to lookup value for in the JSON dictionary",
                "pattern": "the regex pattern the value of `key` should match",
                "flags": "the modifier flags to give to the RegExp object",
            }
        :param str index_name:
            The name of the index
        :param str start_value:
            The start value to search the 2i on
        :param str end_value:
            The end value to search on. Defaults to `None`.
        """
        mr = mgr.riak_map_reduce().index(
            mgr.bucket_name(model), index_name, start_value, end_value).map(
                """
                function(value, keyData, arg) {
                    /*
                        skip deleted values, might show up during a test
                    */
                    var values = value.values.filter(function(val) {
                        return !val.metadata['X-Riak-Deleted'];
                    });
                    if(values.length) {
                        var data = JSON.parse(values[0].data);
                        for (j in arg) {
                            var query = arg[j];
                            var content = data[query.key];
                            var regex = RegExp(query.pattern, query.flags)
                            if(content && regex.test(content)) {
                                return [value.key];
                            }
                        }
                    }
                    return [];
                }
                """, {
                    'arg': query,  # Client lib turns this to JSON for us.
                })
        return cls(mgr, mr)

    @classmethod
    def from_keys(cls, mgr, model, keys):
        bucket_name = mgr.bucket_name(model)
        mr = mgr.riak_map_reduce()
        for key in keys:
            mr.add_bucket_key_data(bucket_name, key, None)
        return cls(mgr, mr)

    def _assert_not_run(self):
        if self._has_run:
            raise VumiMapReduceError("This mapreduce has already run.")
        self._has_run = True

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
        self._assert_not_run()
        self._riak_mapreduce_obj.reduce(
            function=["riak_kv_mapreduce", "reduce_count_inputs"])
        return self._manager.run_map_reduce(
            self._riak_mapreduce_obj, reducer_func=lambda mgr, obj: obj[0])

    def _results_to_keys(self, mgr, obj):
        if isinstance(obj, basestring):
            # Assume strings are keys.
            return obj
        else:
            # If we haven't been given a string, we probably have a riak link.
            _bucket, key, _tag = obj
            return key

    def get_keys(self):
        self._assert_not_run()
        return self._manager.run_map_reduce(
            self._riak_mapreduce_obj, self._results_to_keys)


class Manager(object):
    """A wrapper around a Riak client."""

    DEFAULT_LOAD_BUNCH_SIZE = 100
    DEFAULT_MAPREDUCE_TIMEOUT = 4 * 60 * 1000  # in milliseconds
    # This is a temporary measure to give us an easy way to switch back to the
    # old mechanism if the new one causes problems.
    USE_MAPREDUCE_BUNCH_LOADING = False

    def __init__(self, client, bucket_prefix, load_bunch_size=None,
                 mapreduce_timeout=None, store_versions=None):
        self.client = client
        self.bucket_prefix = bucket_prefix
        self.load_bunch_size = load_bunch_size or self.DEFAULT_LOAD_BUNCH_SIZE
        self.mapreduce_timeout = (mapreduce_timeout or
                                  self.DEFAULT_MAPREDUCE_TIMEOUT)
        self._bucket_cache = {}
        self.store_versions = store_versions or {}

    def proxy(self, modelcls):
        return ModelProxy(self, modelcls)

    def sub_manager(self, sub_prefix):
        return self.__class__(self.client, self.bucket_prefix + sub_prefix)

    def bucket_name(self, modelcls_or_obj):
        return self.bucket_prefix + modelcls_or_obj.bucket

    def bucket_for_modelcls(self, modelcls):
        modelcls_id = id(modelcls)
        bucket = self._bucket_cache.get(modelcls_id)
        if bucket is None:
            bucket_name = self.bucket_name(modelcls)
            bucket = self.riak_bucket(bucket_name)
            self._bucket_cache[modelcls_id] = bucket
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

    def close_manager(self):
        """Close the client underlying this manager instance.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .close_manager(...)")

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

    def load(self, cls, key, result=None):
        """Load a model instance for the key from Riak.

        If the key doesn't exist, this method should return None
        instead of an instance of cls.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .load(...)")

    def _load_multiple(self, cls, keys):
        """Load the model instances for a batch of keys from Riak.

        If a key doesn't exist, no object will be returned for it.
        """
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " ._load_multiple(...)")

    def _load_bunch_mapreduce(self, model, keys):
        """Load the model instances for a batch of keys from Riak.

        If a key doesn't exist, no object will be returned for it.
        """
        mr = self.mr_from_keys(model, keys)
        mr._riak_mapreduce_obj.map(function="""
                function (v) {
                    values = v.values.filter(function(val) {
                        return !val.metadata['X-Riak-Deleted'];
                    })
                    if (!values.length) {
                        return [];
                    }
                    return [[v.key, values[0]]]
                }
                """).filter_not_found()
        return self.run_map_reduce(
            mr._riak_mapreduce_obj, lambda mgr, obj: model.load(mgr, *obj))

    def _load_bunch(self, model, keys):
        """Load the model instances for a batch of keys from Riak.

        If a key doesn't exist, no object will be returned for it.
        """
        assert len(keys) <= self.load_bunch_size
        if not keys:
            return []
        if self.USE_MAPREDUCE_BUNCH_LOADING:
            return self._load_bunch_mapreduce(model, keys)
        else:
            return self._load_multiple(model, keys)

    def load_all_bunches(self, model, keys):
        """Load batches of model instances for a list of keys from Riak.

        :returns:
            An iterator over (possibly deferred) lists of model instances.
        """
        while keys:
            batch_keys = keys[:self.load_bunch_size]
            keys = keys[self.load_bunch_size:]
            yield self._load_bunch(model, batch_keys)

    def riak_map_reduce(self):
        """Construct a RiakMapReduce object for this client."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .riak_map_reduce(...)")

    def run_map_reduce(self, mapreduce, mapper_func=None, reducer_func=None):
        """Run a map reduce instance and return the results mapped to
        objects by the map_function."""
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .run_map_reduce(...)")

    def should_quote_index_values(self):
        raise NotImplementedError("Sub-classes of Manager should implement"
                                  " .should_quote_index_values()")

    def index_keys(self, model, index_name, start_value, end_value=None,
                   return_terms=None):
        bucket = self.bucket_for_modelcls(model)
        if self.should_quote_index_values():
            if start_value is not None:
                start_value = urllib.quote(start_value)
            if end_value is not None:
                end_value = urllib.quote(end_value)
        return bucket.get_index(
            index_name, start_value, end_value, return_terms=return_terms)

    def index_keys_page(self, model, index_name, start_value, end_value=None,
                        return_terms=None, max_results=None,
                        continuation=None):
        bucket = self.bucket_for_modelcls(model)
        if self.should_quote_index_values():
            if start_value is not None:
                start_value = urllib.quote(start_value)
            if end_value is not None:
                end_value = urllib.quote(end_value)
        return bucket.get_index_page(
            index_name, start_value, end_value, return_terms=return_terms,
            max_results=max_results, continuation=continuation)

    def mr_from_field(self, model, field_name, start_value, end_value=None):
        return VumiMapReduce.from_field(
            self, model, field_name, start_value, end_value)

    def mr_from_index(self, model, index_name, start_value, end_value=None):
        return VumiMapReduce.from_index(
            self, model, index_name, start_value, end_value)

    def mr_from_search(self, model, query):
        return VumiMapReduce.from_search(self, model, query)

    def mr_from_index_match(self, model, query, index_name, start_value,
                            end_value=None):
        return VumiMapReduce.from_index_match(self, model, query, index_name,
                                              start_value, end_value)

    def mr_from_field_match(self, model, query, field_name, start_value,
                            end_value=None):
        return VumiMapReduce.from_field_match(self, model, query, field_name,
                                              start_value, end_value)

    def mr_from_keys(self, model, keys):
        return VumiMapReduce.from_keys(self, model, keys)

    def real_search(self, model, query, rows=None, start=None):
        raise NotImplementedError()

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

    def load_all_bunches(self, *args, **kw):
        return self._modelcls.load_all_bunches(self._manager, *args, **kw)

    def all_keys(self):
        return self._modelcls.all_keys(self._manager)

    def index_keys(self, field_name, value, end_value=None, return_terms=None):
        return self._modelcls.index_keys(
            self._manager, field_name, value, end_value,
            return_terms=return_terms)

    def all_keys_page(self, max_results=None, continuation=None):
        return self._modelcls.all_keys_page(
            self._manager, max_results=max_results, continuation=continuation)

    def index_keys_page(self, field_name, value, end_value=None,
                        return_terms=None, max_results=None,
                        continuation=None):
        return self._modelcls.index_keys_page(
            self._manager, field_name, value, end_value,
            return_terms=return_terms, max_results=max_results,
            continuation=continuation)

    def index_lookup(self, field_name, value):
        return self._modelcls.index_lookup(self._manager, field_name, value)

    def index_match(self, query, field_name, value):
        return self._modelcls.index_match(self._manager, query, field_name,
                                          value)

    def search(self, **kw):
        return self._modelcls.search(self._manager, **kw)

    def raw_search(self, query):
        return self._modelcls.raw_search(self._manager, query)

    def real_search(self, query, rows=None, start=None):
        return self._modelcls.real_search(
            self._manager, query, rows=rows, start=start)

    def enable_search(self):
        return self._modelcls.enable_search(self._manager)
