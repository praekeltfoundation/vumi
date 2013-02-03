# -*- test-case-name: vumi.tests.test_config -*-

from zope.interface import Interface
from twisted.python.components import Adapter, registerAdapter

from vumi.errors import ConfigError


class IConfigData(Interface):
    """Interface for a config data provider.

    The default implementation is just a dict. We use interfaces and adapters
    here because we want to be able to easily swap out the implementation to
    handle more dynamic configurations or subclass the interface to allow
    modification of config data.
    """

    def get(field_name, default):
        """Get the value of a config field.

        :returns: The value for the given ``field_name``, or ``default`` if
                  the field has not been specified.
        """

    def has_key(field_name):
        """Check for the existence of a config field.

        :returns: ``True`` if a value exists for the given ``field_name``,
                  ``False`` otherwise.
        """


class DictConfigData(Adapter):
    "Adapter from dict to IConfigData."

    def get(self, field_name, default):
        return self.original.get(field_name, default)

    def has_key(self, field_name):
        return self.original.has_key(field_name)


registerAdapter(DictConfigData, dict, IConfigData)


# TODO: deepcopy or something for list/dict fields? Do we trust the caller to
#       not allow our config to be modified?


class ConfigField(object):
    _creation_order = 0

    def __init__(self, doc, required=False, default=None):
        # This hack is to allow us to track the order in which fields were
        # added to a config class. We want to do this so we can document fields
        # in the same order they're defined.
        self.creation_order = ConfigField._creation_order
        ConfigField._creation_order += 1
        self.name = None
        self.doc = doc
        self.required = required
        self.default = default

    def get_doc(self):
        return ' %s: %s' % (self.name, self.doc)

    def setup(self, name):
        self.name = name

    def validate(self, obj):
        if self.required:
            if not obj._config_data.has_key(self.name):
                raise ConfigError(
                    "Missing required config field '%s'" % (self.name))
        # This will raise an exception if the value exists, but is invalid.
        self.get_value(obj)

    def raise_config_error(self, message_suffix):
        raise ConfigError("Field '%s' %s" % (self.name, message_suffix))

    def clean(self, value):
        return value

    def get_value(self, obj):
        value = obj._config_data.get(self.name, self.default)
        return self.clean(value) if value is not None else None

    def __get__(self, obj, cls):
        return self.get_value(obj)

    def __set__(self, obj, value):
        raise AttributeError("Config fields are read-only.")


class ConfigText(ConfigField):
    def clean(self, value):
        # XXX: We should really differentiate between "unicode" and "bytes".
        #      However, yaml.load() gives us bytestrings or unicode depending
        #      on the content.
        if not isinstance(value, basestring):
            self.raise_config_error("is not unicode.")
        return value


class ConfigInt(ConfigField):
    def clean(self, value):
        try:
            # We go via "str" to avoid silently truncating floats.
            # XXX: Is there a better way to do this?
            return int(str(value))
        except (ValueError, TypeError):
            self.raise_config_error("could not be converted to int.")


class ConfigFloat(ConfigField):
    def clean(self, value):
        try:
            return float(value)
        except (ValueError, TypeError):
            self.raise_config_error("could not be converted to float.")


class ConfigBool(ConfigField):
    def clean(self, value):
        return bool(value)


class ConfigList(ConfigField):
    def clean(self, value):
        if not isinstance(value, (list, tuple)):
            self.raise_config_error("is not a list.")
        return value


class ConfigDict(ConfigField):
    def clean(self, value):
        if not isinstance(value, dict):
            self.raise_config_error("is not a dict.")
        return value


class ConfigMetaClass(type):
    def __new__(mcs, name, bases, dict):
        # locate Field instances
        fields = []
        class_dicts = [dict] + [base.__dict__ for base in reversed(bases)]
        for cls_dict in class_dicts:
            for key, possible_field in cls_dict.items():
                if key in fields:
                    continue
                if isinstance(possible_field, ConfigField):
                    fields.append(possible_field)
                    possible_field.setup(key)

        fields.sort(key=lambda f: f.creation_order)
        dict['fields'] = fields
        cls = type.__new__(mcs, name, bases, dict)
        doc = cls.__doc__ or "Undocumented config!"
        cls.__doc__ = '\n\n'.join(
            [doc] + [field.get_doc() for field in fields])
        return cls


class Config(object):
    """Config object."""

    __metaclass__ = ConfigMetaClass

    def __init__(self, config_data):
        self._config_data = IConfigData(config_data)
        for field in self.fields:
            field.validate(self)
