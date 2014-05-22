# -*- test-case-name: vumi.tests.test_config -*-

from confmodel.config import ConfigText

from vumi.utils import load_class_by_string


class ConfigClassName(ConfigText):
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


class ConfigServerEndpoint(ConfigText):
    field_type = 'twisted_endpoint'

    def __init__(self, *args, **kws):
        fallbacks = kws.pop('fallbacks', ('host', 'port'))
        port_fallback_default = kws.pop('port_fallback_default', None)
        super(ConfigServerEndpoint, self).__init__(*args, **kws)
        self._host_fallback, self._port_fallback = fallbacks
        self._port_fallback_default = port_fallback_default
        self.doc += (" A TCP4 endpoint may be specified using config field"
                     " '%s' for the host and field '%s' for the port but"
                     " this is deprecated"
                     % (self._host_fallback, self._port_fallback))

    def raise_fallback_error(self, fallback_message):
        message_suffix = ("was being specified using the fallback fields '%s'"
                          " and '%s' but "
                          % (self._host_fallback, self._port_fallback))
        message_suffix += fallback_message
        self.raise_config_error(message_suffix)

    def _fallbacks_present(self, obj):
        return (self._host_fallback in obj._config_data or
                self._port_fallback in obj._config_data)

    def _endpoint_from_fallbacks(self, obj):
        host = obj._config_data.get(self._host_fallback, None)
        port = obj._config_data.get(self._port_fallback,
                                    self._port_fallback_default)
        if port is None:
            self.raise_fallback_error('port was not given')
        try:
            port = int(str(port))
        except (ValueError, TypeError):
            self.raise_fallback_error('port was not an integer')
        return self.endpoint_for_addr(host, port)

    def endpoint_for_addr(self, host, port):
        # this does minimal validation and relies on .clean()
        # to check the resulting string and complain if needed
        if host is None:
            return "tcp:port=%s" % (port,)
        else:
            return "tcp:port=%s:interface=%s" % (port, host)

    def present(self, obj):
        return (self._fallbacks_present(obj) or self.name in obj._config_data)

    def get_value(self, obj):
        if self._fallbacks_present(obj):
            value = self._endpoint_from_fallbacks(obj)
        else:
            value = obj._config_data.get(self.name, self.default)
        return self.clean(value) if value is not None else None

    def clean(self, value):
        from twisted.internet.endpoints import serverFromString
        from twisted.internet import reactor
        try:
            return serverFromString(reactor, value)
        except ValueError:
            self.raise_config_error('is not a valid server endpoint')


class ConfigClientEndpoint(ConfigServerEndpoint):
    def endpoint_for_addr(self, host, port):
        # this does minimal validation and relies on .clean()
        # to check the resulting string and complain if needed
        if host is None:
            self.raise_fallback_error('no host was specified')
        return "tcp:host=%s:port=%s" % (host, port)

    def clean(self, value):
        from twisted.internet.endpoints import clientFromString
        from twisted.internet import reactor
        try:
            return clientFromString(reactor, value)
        except ValueError:
            self.raise_config_error('is not a valid client endpoint')


class ConfigContext(object):
    """Context within which a configuration object can be retrieved.

    For example, configuration may depend on the message being processed
    or on the HTTP URL being accessed.
    """
    def __init__(self, **kw):
        for k, v in kw.items():
            setattr(self, k, v)


# Re-export these for compatibility.
from confmodel.config import (
    Config, ConfigField, ConfigInt, ConfigFloat, ConfigBool, ConfigList,
    ConfigDict, ConfigUrl, ConfigRegex)
from confmodel.errors import ConfigError

Config
ConfigField
ConfigInt
ConfigFloat
ConfigBool
ConfigList,
ConfigDict
ConfigUrl
ConfigRegex
ConfigError
