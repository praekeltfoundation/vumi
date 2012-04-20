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
        modelobj._data[self.key] = value

    def retrieve(self, modelobj):
        return modelobj._data[self.key]

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
    pass


class Unicode(Field):
    pass


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
