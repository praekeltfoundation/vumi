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

    def store(self, modelobj, value):
        modelobj._riak_object._data[self.key] = value

    def retrieve(self, modelobj):
        return modelobj._riak_object._data[self.key]

    def __get__(self, instance, owner):
        if instance is None:
            return self.field
        return self.retrieve(instance)

    def __set__(self, instance, value):
        if instance is None:
            raise AttributeError("Attribute %r of %r is not writable" %
                                 (self.key, self.cls))
        self.validate(value)
        self.store(instance, value)


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

    def retrieve(self, modelobj):
        return DynamicProxy(self, modelobj)

    def store(self, modelobj, value):
        raise RuntimeError("DynamicDescriptors should never be assigned to.")

    def store_dynamic(self, modelobj, dynamic_key, value):
        self.field.field_type.validate(value)
        key = self.field.prefix + dynamic_key
        modelobj._riak_object._data[key] = value

    def retrieve_dynamic(self, modelobj, dynamic_key):
        key = self.field.prefix + dynamic_key
        return modelobj._riak_object._data[key]


class DynamicProxy(object):
    def __init__(self, descriptor, modelobj):
        self.__dict__['_descriptor_modelobj_'] = (descriptor, modelobj)

    def __getattr__(self, key):
        descriptor, modelobj = self._descriptor_modelobj_
        return descriptor.retrieve_dynamic(modelobj, key)

    def __setattr__(self, key, value):
        descriptor, modelobj = self._descriptor_modelobj_
        descriptor.store_dynamic(modelobj, key, value)


class Dynamic(Field):
    """A field that allows sub-fields to be added dynamically."""

    descriptor_class = DynamicDescriptor

    def __init__(self, field_type=None, prefix="dynamic."):
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

    def validate(self, value):
        if not isinstance(value, self.othercls):
            raise ValidationError("Field %r of %r requires a %r" %
                                  (self.key, self.cls, self.othercls))

    def store(self, modelobj, value):
        # TODO: write this value to a secondary index or something
        pass

    def retrieve(self, modelobj):
        # TODO: get this value from somewhere
        return None


class ForeignKey(Field):
    descriptor_class = ForeignKeyDescriptor

    def __init__(self, othercls, index=None):
        self._othercls = othercls
        self._index = index
