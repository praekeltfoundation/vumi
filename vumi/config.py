# -*- test-case-name: vumi.tests.test_config -*-

from vumi.errors import ConfigError


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
            if not obj.has_config_value(self.name):
                raise ConfigError(
                    "Missing required config field '%s'" % (self.name))

    def convert(self, value):
        return value

    def __get__(self, obj, cls):
        value = obj.get_config_value(self.name, self.default)
        return self.convert(value) if value is not None else None

    def __set__(self, obj, value):
        raise AttributeError("Config fields are read-only.")


class ConfigString(ConfigField):
    def convert(self, value):
        if isinstance(value, basestring):
            return value
        return unicode(value)


class ConfigInt(ConfigField):
    def convert(self, value):
        return int(value)


class ConfigFloat(ConfigField):
    def convert(self, value):
        return float(value)


class ConfigBool(ConfigField):
    def convert(self, value):
        return bool(value)


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

    def __init__(self, config_dict):
        self._config_dict = config_dict
        for field in self.fields:
            field.validate(self)

    def get_config_value(self, name, default=None):
        # We rely on validation to handle missing required fields.
        return self._config_dict.get(name, default)

    def has_config_value(self, name):
        return name in self._config_dict
