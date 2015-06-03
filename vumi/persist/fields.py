# -*- test-case-name: vumi.persist.tests.test_fields -*-

"""Field types for Vumi's persistence models."""

import iso8601
from datetime import datetime

from vumi.message import format_vumi_date, parse_vumi_date
from vumi.utils import to_kwargs


# Index values in Riak have to be non-empty, so a zero-length string
# counts as "no value". Since we still have legacy data that was
# inadvertantly indexed with "None" because of the str() call in the
# library and we still have legacy code that relies on an index search
# for a value of "None", fixing this properly here will break existing
# functionality. Once we have rewritten the offending code to not use
# "None" in the index, we can remove the hack below and be happier.
STORE_NONE_FOR_EMPTY_INDEX = False


class ValidationError(Exception):
    """Raised when a value assigned to a field is invalid."""


class FieldDescriptor(object):
    """Property for getting and setting fields."""

    def __init__(self, key, field):
        self.key = key
        self.field = field
        if self.field.index:
            if self.field.index_name is None:
                self.index_name = "%s_bin" % self.key
            else:
                self.index_name = field.index_name
        else:
            self.index_name = None

    def setup(self, model_cls):
        self.model_cls = model_cls

    def validate(self, value):
        self.field.validate(value)

    def initialize(self, modelobj, value):
        self.__set__(modelobj, value)

    def _add_index(self, modelobj, value):
        # XXX: The underlying libraries call str() on whatever index values we
        # provide, so we do this explicitly here and special-case None.
        if value is None:
            if STORE_NONE_FOR_EMPTY_INDEX:
                # Hackery for things that need "None" index values.
                modelobj._riak_object.add_index(self.index_name, "None")
            return
        modelobj._riak_object.add_index(self.index_name, str(value))

    def set_value(self, modelobj, value):
        """Set the value associated with this descriptor."""
        raw_value = self.field.to_riak(value)
        modelobj._riak_object.set_data_field(self.key, raw_value)
        if self.index_name is not None:
            modelobj._riak_object.remove_index(self.index_name)
            self._add_index(modelobj, raw_value)

    def get_value(self, modelobj):
        """Get the value associated with this descriptor."""
        raw_value = modelobj._riak_object.get_data().get(self.key)
        return self.field.from_riak(raw_value)

    def clean(self, modelobj):
        """Do any cleanup of the model data for this descriptor after loading
        the data from Riak."""
        pass

    def __repr__(self):
        return "<%s key=%s field=%r>" % (self.__class__.__name__, self.key,
                                         self.field)

    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        return self.get_value(instance)

    def __set__(self, instance, value):
        # instance can never be None here
        self.validate(value)
        self.set_value(instance, value)


class Field(object):
    """Base class for model attributes / fields.

    :param object default:
        Default value for the field. The default default is None.
    :param boolean null:
        Whether None is allowed as a value. Default is False (which
        means the field must either be specified explicitly or by
        a non-None default).
    :param boolen index:
        Whether the field should also be indexed. Default is False.
    :param string index_name:
        The name to use for the index. The default is the field name
        followed by _bin.
    """

    descriptor_class = FieldDescriptor
    # whether an attempt should be made to initialize the field on
    # model instance creation
    initializable = True

    def __init__(self, default=None, null=False, index=False, index_name=None):
        self.default = default
        self.null = null
        self.index = index
        self.index_name = index_name

    def get_descriptor(self, key):
        return self.descriptor_class(key, self)

    def validate(self, value):
        """Validate a value.

        Checks null values and calls .validate() for non-null
        values. Raises ValidationError if a value is invalid.
        """
        if not self.null and value is None:
            raise ValidationError("None is not allowed as a value for non-null"
                                  " fields.")
        if value is not None:
            self.custom_validate(value)

    def custom_validate(self, value):
        """Check whether a non-null value is valid for this field."""
        pass

    def to_riak(self, value):
        return self.custom_to_riak(value) if value is not None else None

    def custom_to_riak(self, value):
        """Convert a non-None value to something storable by Riak."""
        return value

    def from_riak(self, raw_value):
        return (self.custom_from_riak(raw_value)
                if raw_value is not None else None)

    def custom_from_riak(self, raw_value):
        """Convert a non-None value stored by Riak to Python."""
        return raw_value


class Integer(Field):
    """Field that accepts integers.

    Additional parameters:

    :param integer min:
        Minimum allowed value (default is `None` which indicates no minimum).
    :param integer max:
        Maximum allowed value (default is `None` which indicates no maximum).
    """
    def __init__(self, min=None, max=None, **kw):
        super(Integer, self).__init__(**kw)
        self.min = min
        self.max = max

    def custom_validate(self, value):
        if not isinstance(value, (int, long)):
            raise ValidationError("Value %r is not an integer." % (value,))
        if self.min is not None and value < self.min:
            raise ValidationError("Value %r too low (minimum value is %d)."
                                  % (value, self.min))
        if self.max is not None and value > self.max:
            raise ValidationError("Value %r too high (maximum value is %d)."
                                  % (value, self.max))


class Boolean(Field):
    """Field that is either True or False.
    """

    def custom_validate(self, value):
        if not isinstance(value, bool):
            raise ValidationError('Value %r is not a boolean.' % (value,))


class Unicode(Field):
    """Field that accepts unicode strings.

    Additional parameters:

    :param integer max_length:
        Maximum allowed length (default is `None` which indicates no maximum).
    """
    def __init__(self, max_length=None, **kw):
        super(Unicode, self).__init__(**kw)
        self.max_length = max_length

    def custom_validate(self, value):
        if not isinstance(value, unicode):
            raise ValidationError("Value %r is not a unicode string."
                                  % (value,))
        if self.max_length is not None and len(value) > self.max_length:
            raise ValidationError("Value %r too long (maximum length is %d)."
                                  % (value, self.max_length))


class Tag(Field):
    """Field that represents a Vumi tag."""
    def custom_validate(self, value):
        if not isinstance(value, tuple) or len(value) != 2:
            raise ValidationError("Tags %r should be a (pool, tag_name)"
                                  " tuple" % (value,))

    def custom_to_riak(self, value):
        return list(value)

    def custom_from_riak(self, value):
        return tuple(value)


class TimestampDescriptor(FieldDescriptor):
    """A field descriptor for timestamp fields."""

    def set_value(self, modelobj, value):
        if value is not None and not isinstance(value, datetime):
            # we can be sure that this is a iso8601 parseable string, since it
            # passed validation
            value = iso8601.parse_date(value)
        super(TimestampDescriptor, self).set_value(modelobj, value)


class Timestamp(Field):
    """Field that stores a datetime."""

    descriptor_class = TimestampDescriptor

    def custom_validate(self, value):
        if isinstance(value, datetime):
            return

        try:
            iso8601.parse_date(value)
            return
        except iso8601.ParseError:
            pass

        raise ValidationError("Timestamp field expects a datetime or an "
                              "iso8601 formatted string.")

    def custom_to_riak(self, value):
        return format_vumi_date(value)

    def custom_from_riak(self, value):
        return parse_vumi_date(value)


class Json(Field):
    """Field that stores an object that can be serialized to/from JSON."""
    pass


class VumiMessageDescriptor(FieldDescriptor):
    """Property for getting and setting fields."""

    def setup(self, model_cls):
        super(VumiMessageDescriptor, self).setup(model_cls)
        self.message_class = self.field.message_class
        if self.field.prefix is None:
            self.prefix = "%s." % self.key
        else:
            self.prefix = self.field.prefix

    def _clear_keys(self, modelobj):
        for key in modelobj._riak_object.get_data().keys():
            if key.startswith(self.prefix):
                modelobj._riak_object.delete_data_field(key)

    def _timestamp_to_json(self, dt):
        return format_vumi_date(dt)

    def _timestamp_from_json(self, value):
        return parse_vumi_date(value)

    def set_value(self, modelobj, msg):
        """Set the value associated with this descriptor."""
        self._clear_keys(modelobj)
        if msg is None:
            return
        for key, value in msg.payload.iteritems():
            if key == self.message_class._CACHE_ATTRIBUTE:
                continue
            # TODO: timestamp as datetime in payload must die.
            if key == "timestamp":
                value = self._timestamp_to_json(value)
            full_key = "%s%s" % (self.prefix, key)
            modelobj._riak_object.set_data_field(full_key, value)

    def get_value(self, modelobj):
        """Get the value associated with this descriptor."""
        payload = {}
        for key, value in modelobj._riak_object.get_data().iteritems():
            if key.startswith(self.prefix):
                key = key[len(self.prefix):]
                # TODO: timestamp as datetime in payload must die.
                if key == "timestamp":
                    value = self._timestamp_from_json(value)
                payload[key] = value
        if not payload:
            return None
        return self.field.message_class(**to_kwargs(payload))


class VumiMessage(Field):
    """Field that represents a Vumi message.

    Additional parameters:

    :param class message_class:
        The class of the message objects being stored.
        Usually one of Message, TransportUserMessage or TransportEvent.
    :param string prefix:
        The prefix to use when storing message payload keys in Riak. Default is
        the name of the field followed by a dot ('.').

    Note::

       The special message attribute ``__cache__`` is not stored by this
       field.
    """
    descriptor_class = VumiMessageDescriptor

    def __init__(self, message_class, prefix=None, **kw):
        super(VumiMessage, self).__init__(**kw)
        self.message_class = message_class
        self.prefix = prefix

    def custom_validate(self, value):
        if not isinstance(value, self.message_class):
            raise ValidationError("Message %r should be an instance of %r"
                                  % (value, self.message_class))


class FieldWithSubtype(Field):
    """Base class for a field that is a collection of other fields of a
    single type.

    :param Field field_type:
        The field specification for the dynamic values. Default is Unicode().
    """
    def __init__(self, field_type=None, **kw):
        super(FieldWithSubtype, self).__init__(**kw)
        if field_type is None:
            field_type = Unicode()
        if field_type.descriptor_class is not FieldDescriptor:
            raise RuntimeError("Dynamic fields only supports fields that"
                               " that use the basic FieldDescriptor class")
        self.field_type = field_type

    def validate_subfield(self, value):
        self.field_type.validate(value)

    def subfield_to_riak(self, value):
        return self.field_type.to_riak(value)

    def subfield_from_riak(self, value):
        return self.field_type.from_riak(value)


class DynamicDescriptor(FieldDescriptor):
    """A field descriptor for dynamic fields."""
    def setup(self, model_cls):
        super(DynamicDescriptor, self).setup(model_cls)
        if self.field.prefix is None:
            self.prefix = "%s." % self.key
        else:
            self.prefix = self.field.prefix

    def initialize(self, modelobj, valuedict):
        if valuedict is not None:
            self.update(modelobj, valuedict)

    def get_value(self, modelobj):
        return DynamicProxy(self, modelobj)

    def set_value(self, modelobj, valuedict):
        self.clear(modelobj)
        self.update(modelobj, valuedict)

    def clear(self, modelobj):
        keys = list(self.iterkeys(modelobj))
        for key in keys:
            self.delete_dynamic_value(modelobj, key)

    def iterkeys(self, modelobj):
        prefix_len = len(self.prefix)
        data = modelobj._riak_object.get_data()
        return (key[prefix_len:]
                for key in data.iterkeys()
                if key.startswith(self.prefix))

    def iteritems(self, modelobj):
        prefix_len = len(self.prefix)
        from_riak = self.field.subfield_from_riak
        data = modelobj._riak_object.get_data()
        return ((key[prefix_len:], from_riak(value))
                for key, value in data.iteritems()
                if key.startswith(self.prefix))

    def update(self, modelobj, otherdict):
        # this is a separate method so it can succeed or fail
        # somewhat atomically in the case where otherdict contains
        # bad keys or values
        items = [(self.prefix + key, self.field.subfield_to_riak(value))
                 for key, value in otherdict.iteritems()]
        for key, value in items:
            modelobj._riak_object.set_data_field(key, value)

    def get_dynamic_value(self, modelobj, dynamic_key):
        key = self.prefix + dynamic_key
        return self.field.subfield_from_riak(
            modelobj._riak_object.get_data().get(key))

    def set_dynamic_value(self, modelobj, dynamic_key, value):
        self.field.validate_subfield(value)
        key = self.prefix + dynamic_key
        value = self.field.subfield_to_riak(value)
        modelobj._riak_object.set_data_field(key, value)

    def delete_dynamic_value(self, modelobj, dynamic_key):
        key = self.prefix + dynamic_key
        modelobj._riak_object.delete_data_field(key)

    def has_dynamic_key(self, modelobj, dynamic_key):
        key = self.prefix + dynamic_key
        return key in modelobj._riak_object.get_data()


class DynamicProxy(object):
    def __init__(self, descriptor, modelobj):
        self._descriptor = descriptor
        self._modelobj = modelobj

    def iterkeys(self):
        return self._descriptor.iterkeys(self._modelobj)

    def keys(self):
        return list(self.iterkeys())

    def iteritems(self):
        return self._descriptor.iteritems(self._modelobj)

    def items(self):
        return list(self.iteritems())

    def itervalues(self):
        return (value for _key, value in self.iteritems())

    def values(self):
        return list(self.itervalues())

    def update(self, otherdict):
        return self._descriptor.update(self._modelobj, otherdict)

    def clear(self):
        self._descriptor.clear(self._modelobj)

    def copy(self):
        return dict(self.iteritems())

    def __getitem__(self, key):
        return self._descriptor.get_dynamic_value(self._modelobj, key)

    def __setitem__(self, key, value):
        self._descriptor.set_dynamic_value(self._modelobj, key, value)

    def __delitem__(self, key):
        self._descriptor.delete_dynamic_value(self._modelobj, key)

    def __contains__(self, key):
        return self._descriptor.has_dynamic_key(self._modelobj, key)


class Dynamic(FieldWithSubtype):
    """A field that allows sub-fields to be added dynamically.

    :param Field field_type:
        The field specification for the dynamic values. Default is Unicode().
    :param string prefix:
        The prefix to use when storing these values in Riak. Default is
        the name of the field followed by a dot ('.').
    """
    descriptor_class = DynamicDescriptor

    def __init__(self, field_type=None, prefix=None):
        super(Dynamic, self).__init__(field_type=field_type)
        self.prefix = prefix

    def custom_validate(self, valuedict):
        if not isinstance(valuedict, dict):
            raise ValidationError(
                "Value %r should be a dict of subfield name-value pairs"
                % valuedict)
        for key, value in valuedict.iteritems():
            self.validate_subfield(value)
            if not isinstance(key, unicode):
                raise ValidationError("Dynamic field needs unicode keys.")


class ListOfDescriptor(FieldDescriptor):
    """A field descriptor for ListOf fields."""

    def get_value(self, modelobj):
        return ListProxy(self, modelobj)

    def get_list_item(self, modelobj, list_idx):
        raw_list = modelobj._riak_object.get_data().get(self.key, [])
        raw_item = raw_list[list_idx]
        return self.field.subfield_from_riak(raw_item)

    def _set_model_data(self, modelobj, raw_values):
        modelobj._riak_object.set_data_field(self.key, raw_values)
        if self.index_name is not None:
            modelobj._riak_object.remove_index(self.index_name)
            for value in raw_values:
                self._add_index(modelobj, value)

    def set_value(self, modelobj, values):
        map(self.field.validate_subfield, values)
        raw_values = [self.field.subfield_to_riak(value) for value in values]
        self._set_model_data(modelobj, raw_values)

    def set_list_item(self, modelobj, list_idx, value):
        self.field.validate_subfield(value)
        raw_value = self.field.subfield_to_riak(value)
        field_list = modelobj._riak_object.get_data().get(self.key, [])
        field_list[list_idx] = raw_value
        self._set_model_data(modelobj, field_list)

    def del_list_item(self, modelobj, list_idx):
        field_list = modelobj._riak_object.get_data().get(self.key, [])
        del field_list[list_idx]
        self._set_model_data(modelobj, field_list)

    def append_list_item(self, modelobj, value):
        self.field.validate_subfield(value)
        raw_value = self.field.subfield_to_riak(value)
        field_list = modelobj._riak_object.get_data().get(self.key, [])
        field_list.append(raw_value)
        self._set_model_data(modelobj, field_list)

    def remove_list_item(self, modelobj, value):
        self.field.validate_subfield(value)
        raw_value = self.field.subfield_to_riak(value)
        field_list = modelobj._riak_object.get_data().get(self.key, [])
        field_list.remove(raw_value)
        self._set_model_data(modelobj, field_list)

    def extend_list(self, modelobj, values):
        map(self.field.validate_subfield, values)
        raw_values = [self.field.subfield_to_riak(value) for value in values]
        field_list = modelobj._riak_object.get_data().get(self.key, [])
        field_list.extend(raw_values)
        self._set_model_data(modelobj, field_list)

    def iter_list(self, modelobj):
        raw_list = modelobj._riak_object.get_data().get(self.key, [])
        for raw_value in raw_list:
            yield self.field.subfield_from_riak(raw_value)


class ListProxy(object):
    def __init__(self, descriptor, modelobj):
        self._descriptor = descriptor
        self._modelobj = modelobj

    def __getitem__(self, idx):
        return self._descriptor.get_list_item(self._modelobj, idx)

    def __setitem__(self, idx, value):
        self._descriptor.set_list_item(self._modelobj, idx, value)

    def __delitem__(self, idx):
        self._descriptor.del_list_item(self._modelobj, idx)

    def remove(self, value):
        self._descriptor.remove_list_item(self._modelobj, value)

    def append(self, value):
        self._descriptor.append_list_item(self._modelobj, value)

    def extend(self, values):
        self._descriptor.extend_list(self._modelobj, values)

    def __iter__(self):
        return self._descriptor.iter_list(self._modelobj)


class ListOf(FieldWithSubtype):
    """A field that contains a list of values of some other type.

    :param Field field_type:
    The field specification for the dynamic values. Default is Unicode().
    """
    descriptor_class = ListOfDescriptor

    def __init__(self, field_type=None, **kw):
        super(ListOf, self).__init__(field_type=field_type, default=list, **kw)

    def custom_validate(self, valuelist):
        if not isinstance(valuelist, list):
            raise ValidationError(
                "Value %r should be a list of values" % valuelist)
        map(self.validate_subfield, valuelist)


class SetOfDescriptor(FieldDescriptor):
    """
    A field descriptor for SetOf fields.
    """

    def get_value(self, modelobj):
        return SetProxy(self, modelobj)

    def _get_model_data(self, modelobj):
        return set(modelobj._riak_object.get_data().get(self.key, []))

    def _set_model_data(self, modelobj, raw_values):
        raw_values = sorted(set(raw_values))
        modelobj._riak_object.set_data_field(self.key, raw_values)
        if self.index_name is not None:
            modelobj._riak_object.remove_index(self.index_name)
            for value in raw_values:
                self._add_index(modelobj, value)

    def set_contains_item(self, modelobj, value):
        field_set = self._get_model_data(modelobj)
        return value in field_set

    def set_value(self, modelobj, values):
        map(self.field.validate_subfield, values)
        raw_values = [self.field.subfield_to_riak(value) for value in values]
        self._set_model_data(modelobj, raw_values)

    def add_set_item(self, modelobj, value):
        self.field.validate_subfield(value)
        field_set = self._get_model_data(modelobj)
        field_set.add(self.field.subfield_to_riak(value))
        self._set_model_data(modelobj, field_set)

    def remove_set_item(self, modelobj, value):
        self.field.validate_subfield(value)
        field_set = self._get_model_data(modelobj)
        field_set.remove(value)
        self._set_model_data(modelobj, field_set)

    def discard_set_item(self, modelobj, value):
        self.field.validate_subfield(value)
        field_set = self._get_model_data(modelobj)
        field_set.discard(value)
        self._set_model_data(modelobj, field_set)

    def update_set(self, modelobj, values):
        map(self.field.validate_subfield, values)
        raw_values = [self.field.subfield_to_riak(value) for value in values]
        field_set = self._get_model_data(modelobj)
        field_set.update(raw_values)
        self._set_model_data(modelobj, field_set)

    def iter_set(self, modelobj):
        field_set = self._get_model_data(modelobj)
        for raw_value in field_set:
            yield self.field.subfield_from_riak(raw_value)


class SetProxy(object):
    def __init__(self, descriptor, modelobj):
        self._descriptor = descriptor
        self._modelobj = modelobj

    def __contains__(self, value):
        return self._descriptor.set_contains_item(self._modelobj, value)

    def add(self, value):
        self._descriptor.add_set_item(self._modelobj, value)

    def remove(self, value):
        self._descriptor.remove_set_item(self._modelobj, value)

    def discard(self, value):
        self._descriptor.discard_set_item(self._modelobj, value)

    def update(self, values):
        self._descriptor.update_set(self._modelobj, values)

    def __iter__(self):
        return self._descriptor.iter_set(self._modelobj)


class SetOf(FieldWithSubtype):
    """
    A field that contains a set of values of some other type.

    :param Field field_type:
        The field specification for the dynamic values. Default is Unicode().
    """
    descriptor_class = SetOfDescriptor

    def __init__(self, field_type=None, **kw):
        super(SetOf, self).__init__(field_type=field_type, default=set, **kw)

    def custom_validate(self, valueset):
        if not isinstance(valueset, set):
            raise ValidationError(
                "Value %r should be a set of values" % valueset)
        map(self.validate_subfield, valueset)

    def custom_to_riak(self, value):
        return sorted(value)

    def custom_from_riak(self, raw_value):
        return set(raw_value)


class ForeignKeyDescriptor(FieldDescriptor):
    def setup(self, model_cls):
        super(ForeignKeyDescriptor, self).setup(model_cls)
        self.other_model = self.field.other_model
        if self.field.index is None:
            self.index_name = "%s_bin" % self.key
        else:
            self.index_name = self.field.index

        backlink_name = self.field.backlink
        if backlink_name is None:
            backlink_name = model_cls.__name__.lower() + "s"
        self.other_model.backlinks.declare_backlink(
            backlink_name, self.reverse_lookup_keys)

        backlink_keys_name = backlink_name + "_keys"
        if backlink_keys_name.endswith("s_keys"):
            backlink_keys_name = backlink_name[:-1] + "_keys"
        self.other_model.backlinks.declare_backlink(
            backlink_keys_name, self.reverse_lookup_keys_paginated)

    def reverse_lookup_keys(self, modelobj, manager=None):
        if manager is None:
            manager = modelobj.manager
        return manager.index_keys(
            self.model_cls, self.index_name, modelobj.key)

    def reverse_lookup_keys_paginated(self, modelobj, manager=None,
                                      max_results=None, continuation=None):
        """
        Perform a paginated index query for backlinked objects.
        """
        if manager is None:
            manager = modelobj.manager
        return manager.index_keys_page(
            self.model_cls, self.index_name, modelobj.key,
            max_results=max_results, continuation=continuation)

    def clean(self, modelobj):
        if self.key not in modelobj._riak_object.get_data():
            # We might have an old-style index-only version of the data.
            indexes = [
                value for name, value in modelobj._riak_object.get_indexes()
                if name == self.index_name]
            modelobj._riak_object.set_data_field(
                self.key, (indexes or [None])[0])

    def get_value(self, modelobj):
        return ForeignKeyProxy(self, modelobj)

    def get_foreign_key(self, modelobj):
        return modelobj._riak_object.get_data().get(self.key)

    def set_foreign_key(self, modelobj, foreign_key):
        modelobj._riak_object.set_data_field(self.key, foreign_key)
        modelobj._riak_object.remove_index(self.index_name)
        if foreign_key is not None:
            self._add_index(modelobj, foreign_key)

    def get_foreign_object(self, modelobj, manager=None):
        key = self.get_foreign_key(modelobj)
        if key is None:
            return None
        if manager is None:
            manager = modelobj.manager
        return self.other_model.load(manager, key)

    def initialize(self, modelobj, value):
        if isinstance(value, basestring):
            self.set_foreign_key(modelobj, value)
        else:
            self.set_foreign_object(modelobj, value)

    def set_value(self, modelobj, value):
        raise RuntimeError("ForeignKeyDescriptors should never be assigned"
                           " to.")

    def set_foreign_object(self, modelobj, otherobj):
        self.validate(otherobj)
        foreign_key = otherobj.key if otherobj is not None else None
        self.set_foreign_key(modelobj, foreign_key)


class ForeignKeyProxy(object):
    def __init__(self, descriptor, modelobj):
        self._descriptor = descriptor
        self._modelobj = modelobj

    def _get_key(self):
        return self._descriptor.get_foreign_key(self._modelobj)

    def _set_key(self, foreign_key):
        return self._descriptor.set_foreign_key(self._modelobj, foreign_key)

    key = property(fget=_get_key, fset=_set_key)

    def get(self, manager=None):
        return self._descriptor.get_foreign_object(self._modelobj, manager)

    def set(self, otherobj):
        self._descriptor.set_foreign_object(self._modelobj, otherobj)


class ForeignKey(Field):
    """A field that links to another class.

    Additional parameters:

    :param Model other_model:
        The type of model linked to.
    :param string index:
        The name to use for the index. The default is the field name
        followed by _bin.
    :param string backlink:
        The name to use for the backlink on :attr:`other_model.backlinks`.
        The default is the name of the class the field is on converted
        to lowercase and with 's' appended (e.g. 'FooModel' would result
        in :attr:`other_model.backlinks.foomodels`). This is also used (with
        `_keys` appended and a trailing `s` omitted if one is present) for the
        paginated keys backlink function.
    """
    descriptor_class = ForeignKeyDescriptor

    def __init__(self, other_model, index=None, backlink=None, **kw):
        super(ForeignKey, self).__init__(**kw)
        self.other_model = other_model
        self.index = index
        self.backlink = backlink

    def custom_validate(self, value):
        if not isinstance(value, self.other_model):
            raise ValidationError("ForeignKey requires a %r instance" %
                                  (self.other_model,))


class ManyToManyDescriptor(ForeignKeyDescriptor):

    def get_value(self, modelobj):
        return ManyToManyProxy(self, modelobj)

    def set_value(self, modelobj, value):
        raise RuntimeError("ManyToManyDescriptors should never be assigned"
                           " to.")

    def clean(self, modelobj):
        if self.key not in modelobj._riak_object.get_data():
            # We might have an old-style index-only version of the data.
            indexes = [
                value for name, value in modelobj._riak_object.get_indexes()
                if name == self.index_name]
            modelobj._riak_object.set_data_field(self.key, indexes[:])

    def get_foreign_keys(self, modelobj):
        return modelobj._riak_object.get_data()[self.key][:]

    def add_foreign_key(self, modelobj, foreign_key):
        if foreign_key not in self.get_foreign_keys(modelobj):
            field_list = modelobj._riak_object.get_data().get(self.key, [])
            field_list.append(foreign_key)
            modelobj._riak_object.set_data_field(self.key, field_list)
        self._add_index(modelobj, foreign_key)

    def remove_foreign_key(self, modelobj, foreign_key):
        if foreign_key in self.get_foreign_keys(modelobj):
            field_list = modelobj._riak_object.get_data().get(self.key, [])
            field_list.remove(foreign_key)
            modelobj._riak_object.set_data_field(self.key, field_list)
        modelobj._riak_object.remove_index(self.index_name, foreign_key)

    def load_foreign_objects(self, modelobj, manager=None):
        keys = self.get_foreign_keys(modelobj)
        if manager is None:
            manager = modelobj.manager
        return manager.load_all_bunches(self.other_model, keys)

    def add_foreign_object(self, modelobj, otherobj):
        self.validate(otherobj)
        self.add_foreign_key(modelobj, otherobj.key)

    def remove_foreign_object(self, modelobj, otherobj):
        self.validate(otherobj)
        self.remove_foreign_key(modelobj, otherobj.key)

    def clear_keys(self, modelobj):
        modelobj._riak_object.set_data_field(self.key, [])
        modelobj._riak_object.remove_index(self.index_name)


class ManyToManyProxy(object):
    def __init__(self, descriptor, modelobj):
        self._descriptor = descriptor
        self._modelobj = modelobj

    def keys(self):
        return self._descriptor.get_foreign_keys(self._modelobj)

    def add_key(self, foreign_key):
        self._descriptor.add_foreign_key(self._modelobj, foreign_key)

    def remove_key(self, foreign_key):
        self._descriptor.remove_foreign_key(self._modelobj, foreign_key)

    def load_all_bunches(self, manager=None):
        return self._descriptor.load_foreign_objects(self._modelobj, manager)

    def add(self, otherobj):
        self._descriptor.add_foreign_object(self._modelobj, otherobj)

    def remove(self, otherobj):
        self._descriptor.remove_foreign_object(self._modelobj, otherobj)

    def clear(self):
        self._descriptor.clear_keys(self._modelobj)


class ManyToMany(ForeignKey):
    """A field that links to multiple instances of another class.

    :param Model other_model:
        The type of model linked to.
    :param string index:
        The name to use for the index. The default is the field name
        followed by _bin.
    :param string backlink:
        The name to use for the backlink on :attr:`other_model.backlinks`.
        The default is the name of the class the field is on converted
        to lowercase and with 's' appended (e.g. 'FooModel' would result
        in :attr:`other_model.backlinks.foomodels`). This is also used (with
        `_keys` appended and a trailing `s` omitted if one is present) for the
        paginated keys backlink function.
    """
    descriptor_class = ManyToManyDescriptor
    initializable = False

    def __init__(self, other_model, index=None, backlink=None):
        super(ManyToMany, self).__init__(other_model, index, backlink)
