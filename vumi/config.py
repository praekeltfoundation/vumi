# -*- test-case-name: vumi.tests.test_config -*-

from confmodel.fields import ConfigField
from confmodel.fallbacks import FieldFallback

from vumi.utils import load_class_by_string

from confmodel import Config
from confmodel.errors import ConfigError
from confmodel.fields import (
    ConfigInt, ConfigFloat, ConfigBool, ConfigList, ConfigDict, ConfigText,
    ConfigUrl, ConfigRegex)
from confmodel.interfaces import IConfigData


class ConfigClassName(ConfigField):
    field_type = 'Class'

    def __init__(self, doc, required=False, default=None, static=False,
                 implements=None):
        super(ConfigClassName, self).__init__(doc, required, default, static)
        self.interface = implements

    def clean(self, value):
        try:
            cls = load_class_by_string(value)
        except (ValueError, ImportError), e:
            # ValueError for empty module name
            self.raise_config_error(str(e))

        if self.interface and not self.interface.implementedBy(cls):
            self.raise_config_error('does not implement %r.' % (
                self.interface,))
        return cls


class ConfigServerEndpoint(ConfigField):
    field_type = 'twisted_endpoint'

    def clean(self, value):
        from twisted.internet.endpoints import serverFromString
        from twisted.internet import reactor
        try:
            return serverFromString(reactor, value)
        except ValueError:
            self.raise_config_error('is not a valid server endpoint')


class ServerEndpointFallback(FieldFallback):
    def __init__(self, host_field="host", port_field="port"):
        self.host_field = host_field
        self.port_field = port_field
        self.required_fields = [port_field]

    def build_value(self, config):
        fields = {
            "host": getattr(config, self.host_field),
            "port": getattr(config, self.port_field),
        }

        formatstr = "tcp:port={port}"
        if fields["host"] is not None:
            formatstr += ":interface={host}"
        return formatstr.format(**fields)


class ConfigClientEndpoint(ConfigField):
    field_type = 'twisted_endpoint'

    def clean(self, value):
        from twisted.internet.endpoints import clientFromString
        from twisted.internet import reactor
        try:
            return clientFromString(reactor, value)
        except ValueError:
            self.raise_config_error('is not a valid client endpoint')


class ClientEndpointFallback(FieldFallback):
    def __init__(self, host_field="host", port_field="port"):
        self.host_field = host_field
        self.port_field = port_field
        self.required_fields = [self.host_field, self.port_field]

    def build_value(self, config):
        fields = {
            "host": getattr(config, self.host_field),
            "port": getattr(config, self.port_field),
        }
        return "tcp:host={host}:port={port}".format(**fields)


class ConfigContext(object):
    """Context within which a configuration object can be retrieved.

    For example, configuration may depend on the message being processed
    or on the HTTP URL being accessed.
    """
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


class ConfigRiak(ConfigDict):
    field_type = 'riak'
    """Riak configuration.

    Ensures that there is at least a ``bucket_prefix`` key.
    """
    def clean(self, value):
        if "bucket_prefix" not in value:
            self.raise_config_error(
                "does not contain the `bucket_prefix` key.")
        return super(self.__class__, self).clean(value)

# Re-export these for compatibility.
Config
ConfigError
ConfigInt
ConfigFloat
ConfigBool
ConfigList
ConfigDict
ConfigText
ConfigUrl
ConfigRegex
IConfigData
