from twisted.internet.endpoints import TCP4ServerEndpoint, TCP4ClientEndpoint

from confmodel import Config
from confmodel.errors import ConfigError
from confmodel.fields import ConfigInt, ConfigText
from zope.interface import Interface, implements

from vumi.config import (
    ConfigClassName, ConfigServerEndpoint, ConfigClientEndpoint,
    ServerEndpointFallback, ClientEndpointFallback)
from vumi.tests.helpers import VumiTestCase


class ITestConfigInterface(Interface):

    def implements_this(foo):
        """This should be implemented"""


class TestConfigClassName(object):
    implements(ITestConfigInterface)

    def implements_this(self, foo):
        pass


class FakeModel(object):
    def __init__(self, config):
        self._config_data = config


class ConfigFieldTest(VumiTestCase):
    def fake_model(self, *value, **kw):
        config = kw.pop('config', {})
        if value:
            assert len(value) == 1
            config['foo'] = value[0]
        return FakeModel(config)

    def field_value(self, field, *value, **kw):
        self.assert_field_valid(field, *value, **kw)
        return field.get_value(self.fake_model(*value, **kw))

    def assert_field_valid(self, field, *value, **kw):
        field.validate(self.fake_model(*value, **kw))

    def assert_field_invalid(self, field, *value, **kw):
        self.assertRaises(ConfigError, field.validate,
                          self.fake_model(*value, **kw))

    def make_field(self, field_cls, **kw):
        field = field_cls("desc", **kw)
        field.setup('foo')
        return field

    def test_classname_field(self):
        field = self.make_field(ConfigClassName)
        klass = self.field_value(field,
                                 'vumi.tests.test_config.TestConfigClassName')
        self.assertEqual(klass, TestConfigClassName)

    def test_invalid_classname_field(self):
        field = self.make_field(ConfigClassName)
        self.assert_field_invalid(field, '0000')
        self.assert_field_invalid(field, '0000.bar')

    def test_classname_implements_field(self):
        field = self.make_field(ConfigClassName,
                                implements=ITestConfigInterface)
        klass = self.field_value(
            field, 'vumi.tests.test_config.TestConfigClassName')
        self.assertEqual(klass, TestConfigClassName)

    def test_invalid_classname_implements_field(self):
        field = self.make_field(ConfigClassName,
                                implements=ITestConfigInterface)
        self.assert_field_invalid(
            field, 'vumi.tests.test_config.ConfigFieldTest')

    def check_endpoint(self, endpoint, endpoint_type, **kw):
        self.assertEqual(type(endpoint), endpoint_type)
        for name, value in kw.items():
            self.assertEqual(getattr(endpoint, '_%s' % name), value)

    def test_server_endpoint_field(self):
        field = self.make_field(ConfigServerEndpoint, required=True)
        self.check_endpoint(self.field_value(
            field, 'tcp:60'),
            TCP4ServerEndpoint, interface='', port=60)
        self.check_endpoint(self.field_value(
            field, 'tcp:interface=127.0.0.1:port=60'),
            TCP4ServerEndpoint, interface='127.0.0.1', port=60)

        self.assert_field_invalid(field)
        self.assert_field_invalid(field, 'foo')

    def test_server_endpoint_field_with_fallback(self):
        class MyConfig(Config):
            host = ConfigText("host")
            port = ConfigInt("port")
            endpoint = ConfigServerEndpoint(
                "endpoint", fallbacks=[ServerEndpointFallback()],
                required=False)

        self.check_endpoint(
            MyConfig({'endpoint': 'tcp:60'}).endpoint,
            TCP4ServerEndpoint, interface='', port=60)
        self.check_endpoint(
            MyConfig({'port': 80}).endpoint,
            TCP4ServerEndpoint, interface='', port=80)
        self.check_endpoint(
            MyConfig({'host': '127.0.0.1', 'port': 80}).endpoint,
            TCP4ServerEndpoint, interface='127.0.0.1', port=80)

        self.assertEqual(MyConfig({}).endpoint, None)
        self.assertEqual(MyConfig({'host': '127.0.0.1'}).endpoint, None)
        self.assertRaises(ConfigError, MyConfig, {'port': 'foo'})

    def test_server_endpoint_field_with_fallback_custom_fields(self):
        class MyConfig(Config):
            myhost = ConfigText("myhost")
            myport = ConfigInt("myport")
            endpoint = ConfigServerEndpoint(
                "endpoint", required=False,
                fallbacks=[ServerEndpointFallback('myhost', 'myport')])

        self.check_endpoint(
            MyConfig({'endpoint': 'tcp:60'}).endpoint,
            TCP4ServerEndpoint, interface='', port=60)
        self.check_endpoint(
            MyConfig({'myport': 80}).endpoint,
            TCP4ServerEndpoint, interface='', port=80)
        self.check_endpoint(
            MyConfig({'myhost': '127.0.0.1', 'myport': 80}).endpoint,
            TCP4ServerEndpoint, interface='127.0.0.1', port=80)

        self.assertEqual(MyConfig({}).endpoint, None)
        self.assertEqual(MyConfig({'myhost': '127.0.0.1'}).endpoint, None)
        self.assertRaises(ConfigError, MyConfig, {'myport': 'foo'})

    def test_client_endpoint_field(self):
        field = self.make_field(ConfigClientEndpoint)
        self.check_endpoint(
            self.field_value(field, 'tcp:127.0.0.1:60'),
            TCP4ClientEndpoint, host='127.0.0.1', port=60)

        self.assertEqual(self.field_value(field), None)
        self.assert_field_invalid(field, 'foo')

    def test_client_endpoint_field_with_fallback(self):
        class MyConfig(Config):
            host = ConfigText("host")
            port = ConfigInt("port")
            endpoint = ConfigClientEndpoint(
                "endpoint", fallbacks=[ClientEndpointFallback()],
                required=True)

        self.check_endpoint(
            MyConfig({'endpoint': 'tcp:example.com:80'}).endpoint,
            TCP4ClientEndpoint, host='example.com', port=80)
        self.check_endpoint(
            MyConfig({'host': 'example.com', 'port': 80}).endpoint,
            TCP4ClientEndpoint, host='example.com', port=80)

        self.assertRaises(ConfigError, MyConfig, {'port': 'foo'})
        self.assertRaises(ConfigError, MyConfig, {'host': 'example.com'})
        self.assertRaises(ConfigError, MyConfig, {'port': 80})

    def test_client_endpoint_field_with_fallback_custom_fields(self):
        class MyConfig(Config):
            myhost = ConfigText("myhost")
            myport = ConfigInt("myport")
            endpoint = ConfigClientEndpoint(
                "endpoint", required=True,
                fallbacks=[ClientEndpointFallback('myhost', 'myport')])

        self.check_endpoint(
            MyConfig({'endpoint': 'tcp:example.com:80'}).endpoint,
            TCP4ClientEndpoint, host='example.com', port=80)
        self.check_endpoint(
            MyConfig({'myhost': 'example.com', 'myport': 80}).endpoint,
            TCP4ClientEndpoint, host='example.com', port=80)

        self.assertRaises(ConfigError, MyConfig, {'myport': 'foo'})
        self.assertRaises(ConfigError, MyConfig, {'myhost': 'example.com'})
        self.assertRaises(ConfigError, MyConfig, {'myport': 80})
