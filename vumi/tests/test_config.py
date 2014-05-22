from twisted.internet.endpoints import TCP4ServerEndpoint, TCP4ClientEndpoint

from confmodel.errors import ConfigError
from vumi.config import (
    ConfigServerEndpoint, ConfigClientEndpoint, ConfigClassName)
from vumi.tests.helpers import VumiTestCase

from zope.interface import Interface, implements


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
        field = self.make_field(ConfigServerEndpoint)
        self.check_endpoint(self.field_value(
            field, 'tcp:60'),
            TCP4ServerEndpoint, interface='', port=60)
        self.check_endpoint(self.field_value(
            field, config={'port': 80}),
            TCP4ServerEndpoint, interface='', port=80)
        self.check_endpoint(self.field_value(
            field, config={'host': '127.0.0.1', 'port': 80}),
            TCP4ServerEndpoint, interface='127.0.0.1', port=80)

        self.assertEqual(self.field_value(field), None)

        self.assert_field_invalid(field, config={'host': '127.0.0.1'})
        self.assert_field_invalid(field, 'foo')

    def test_server_endpoint_field_required(self):
        field = self.make_field(ConfigServerEndpoint, required=True)
        self.check_endpoint(self.field_value(
            field, 'tcp:60'),
            TCP4ServerEndpoint, interface='', port=60)
        self.check_endpoint(self.field_value(
            field, config={'port': 80}),
            TCP4ServerEndpoint, interface='', port=80)

        self.assert_field_invalid(field)

    def test_client_endpoint_field(self):
        field = self.make_field(ConfigClientEndpoint)
        self.check_endpoint(
            self.field_value(field, 'tcp:127.0.0.1:60'),
            TCP4ClientEndpoint, host='127.0.0.1', port=60)
        self.check_endpoint(self.field_value(
            field, config={'host': '127.0.0.1', 'port': 80}),
            TCP4ClientEndpoint, host='127.0.0.1', port=80)

        self.assertEqual(self.field_value(field), None)

        self.assert_field_invalid(field, config={'port': 80})
        self.assert_field_invalid(field, config={'host': '127.0.0.1'})
        self.assert_field_invalid(field, 'foo')

    def test_client_endpoint_field_required(self):
        field = self.make_field(ConfigClientEndpoint, required=True)
        self.check_endpoint(
            self.field_value(field, 'tcp:127.0.0.1:60'),
            TCP4ClientEndpoint, host='127.0.0.1', port=60)
        self.check_endpoint(self.field_value(
            field, config={'host': '127.0.0.1', 'port': 80}),
            TCP4ClientEndpoint, host='127.0.0.1', port=80)

        self.assert_field_invalid(field)

    def test_client_endpoint_field_with_port_fallback(self):
        field = self.make_field(
            ConfigClientEndpoint, port_fallback_default=51)
        self.check_endpoint(
            self.field_value(field, config={'host': '127.0.0.1'}),
            TCP4ClientEndpoint, host='127.0.0.1', port=51)
        self.check_endpoint(self.field_value(
            field, config={'host': '127.0.0.1', 'port': 80}),
            TCP4ClientEndpoint, host='127.0.0.1', port=80)

        self.assert_field_invalid(field, config={'port': 80})
