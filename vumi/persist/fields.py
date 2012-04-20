# -*- test-case-name: vumi.persist.tests.test_fields -*-

"""Field types for Vumi's persistence models."""


class ValidationError(Exception):
    """Raised when a value assigned to a field is invalid."""


class FieldDescriptor(object):
    """Property for getting and setting fields."""

    def __init__(self, key, field):
        self.key = key
        self.field = field

    def setup(self, cls):
        self.cls = cls

    def validate(self, value):
        self.field.validate(value)

    def set_value(self, modelobj, value):
        """Set the value associated with this descriptor."""
        modelobj._riak_object._data[self.key] = value

    def get_value(self, modelobj):
        """Get the value associated with this descriptor."""
        return modelobj._riak_object._data[self.key]

    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        return self.get_value(instance)

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("Attribute %r of %r is not writable" %
                                 (self.key, self.cls))
        self.validate(value)
        self.set_value(instance, value)


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


class Integer(Field):
    """Field that accepts integers.

    :param integer min:
        Minimum allowed value (default is `None` which indicates no minimum).
    :param integer max:
        Maximum allowed value (default is `None` which indicates no maximum).
    """
    def __init__(self, min=None, max=None):
        self.min = min
        self.max = max

    def validate(self, value):
        if not isinstance(value, (int, long)):
            raise ValidationError("Value %r is not an integer." % (value,))
        if self.min is not None and value < self.min:
            raise ValidationError("Value %r too low (minimum value is %d)."
                                  % (value, self.min))
        if self.max is not None and value > self.max:
            raise ValidationError("Value %r too high (maximum value is %d)."
                                  % (value, self.max))


class Unicode(Field):
    """Field that accepts unicode strings."""
    def validate(self, value):
        if not isinstance(value, unicode):
            raise ValidationError("Value %r is not a unicode string."
                                  % (value,))


class DynamicDescriptor(FieldDescriptor):
    """A field descriptor for dynamic fields."""
    def setup(self, cls):
        super(DynamicDescriptor, self).setup(cls)
        if self.field.prefix is None:
            self.prefix = "%s." % self.key
        else:
            self.prefix = self.field.prefix

    def get_value(self, modelobj):
        return DynamicProxy(self, modelobj)

    def get_dynamic_value(self, modelobj, dynamic_key):
        key = self.prefix + dynamic_key
        return modelobj._riak_object._data[key]

    def set_value(self, modelobj, value):
        raise RuntimeError("DynamicDescriptors should never be assigned to.")

    def set_dynamic_value(self, modelobj, dynamic_key, value):
        self.field.field_type.validate(value)
        key = self.prefix + dynamic_key
        modelobj._riak_object._data[key] = value


class DynamicProxy(object):
    def __init__(self, descriptor, modelobj):
        self.__dict__['_descriptor_modelobj_'] = (descriptor, modelobj)

    def __getattr__(self, key):
        descriptor, modelobj = self._descriptor_modelobj_
        return descriptor.get_dynamic_value(modelobj, key)

    def __setattr__(self, key, value):
        descriptor, modelobj = self._descriptor_modelobj_
        descriptor.set_dynamic_value(modelobj, key, value)


class Dynamic(Field):
    """A field that allows sub-fields to be added dynamically.

    :param Field field_type:
        The field specification for the dynamic values. Default is Unicode().
    :param string prefix:
        The prefix to use when storing these values in Riak. Default is
        the name of the field followed by a dot ('.').
    """
    descriptor_class = DynamicDescriptor

    def __init__(self, field_type=None, prefix=None):
        if field_type is None:
            field_type = Unicode()
        if field_type.descriptor_class is not FieldDescriptor:
            raise RuntimeError("Dynamic fields only supports fields that"
                               " that use the basic FieldDescriptor class")
        self.field_type = field_type
        self.prefix = prefix

    def validate(self, value):
        self.field_type.validate(value)


class ForeignKeyDescriptor(FieldDescriptor):
    def setup(self, cls):
        super(ForeignKeyDescriptor, self).setup(cls)
        self.othercls = self.field.othercls
        if self.field.index is None:
            self.index_name = "%s_bin" % self.key
        else:
            self.index_name = self.field.index

    def validate(self, value):
        if not isinstance(value, self.othercls):
            raise ValidationError("Field %r of %r requires a %r" %
                                  (self.key, self.cls, self.othercls))

    def get_value(self, modelobj):
        return ForeignKeyProxy(self, modelobj)

    def get_foreign_key(self, modelobj):
        indexes = modelobj._riak_object.get_indexes(self.index_name)
        if not indexes:
            return None
        key = indexes[0]
        return key

    def get_foreign_object(self, modelobj):
        key = self.get_foreign_key(modelobj)
        if key is None:
            return None
        return self.othercls.load(modelobj.manager, key)

    def set_value(self, modelobj, value):
        raise RuntimeError("ForeignKeyDescriptors should never be assigned"
                           " to.")

    def set_foreign_object(self, modelobj, otherobj):
        modelobj._riak_object.remove_index(self.index_name)
        modelobj._riak_object.add_index(self.index_name, otherobj.key)


class ForeignKeyProxy(object):
    def __init__(self, descriptor, modelobj):
        self._descriptor = descriptor
        self._modelobj = modelobj

    @property
    def key(self):
        return self._descriptor.get_foreign_key(self._modelobj)

    def get(self):
        return self._descriptor.get_foreign_object(self._modelobj)

    def set(self, otherobj):
        self._descriptor.set_foreign_object(self._modelobj, otherobj)


class ForeignKey(Field):
    """A field that links to another class.

    :param Model othercls:
        The type of model linked to.
    :param string index:
        The name to use for the index. The default is the field name
        followed by _bin.
    """
    descriptor_class = ForeignKeyDescriptor

    def __init__(self, othercls, index=None):
        self.othercls = othercls
        self.index = index
