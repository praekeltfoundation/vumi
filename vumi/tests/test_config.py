from twisted.internet.endpoints import TCP4ServerEndpoint, TCP4ClientEndpoint

from vumi.errors import ConfigError
from vumi.config import (
    Config, ConfigField, ConfigText, ConfigInt, ConfigFloat, ConfigBool,
    ConfigList, ConfigDict, ConfigUrl, ConfigRegex, ConfigServerEndpoint,
    ConfigClientEndpoint, ConfigClassName)
from vumi.tests.helpers import VumiTestCase

from zope.interface import Interface, implements


class ITestConfigInterface(Interface):

    def implements_this(foo):
        """This should be implemented"""


class TestConfigClassName(object):
    implements(ITestConfigInterface)

    def implements_this(self, foo):
        pass


class ConfigTest(VumiTestCase):
    def test_simple_config(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("foo")
            bar = ConfigField("bar")

        conf = FooConfig({'foo': 'blah'})
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, None)

        conf = FooConfig({'bar': 'blah'})
        self.assertEqual(conf.foo, None)
        self.assertEqual(conf.bar, 'blah')

    def test_required_field(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("foo", required=True)
            bar = ConfigField("bar")

        conf = FooConfig({'foo': 'blah'})
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, None)

        self.assertRaises(ConfigError, FooConfig, {'bar': 'blah'})

    def test_static_field(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("foo", required=True, static=True)
            bar = ConfigField("bar")

        conf = FooConfig({'foo': 'blah', 'bar': 'baz'}, static=True)
        self.assertEqual(conf.foo, 'blah')
        self.assertRaises(ConfigError, lambda: conf.bar)

    def test_default_field(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("what 'twas", default="brillig")
            bar = ConfigField("tove status", default="slithy")

        conf = FooConfig({'foo': 'blah'})
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, 'slithy')

        conf = FooConfig({'bar': 'blah'})
        self.assertEqual(conf.foo, 'brillig')
        self.assertEqual(conf.bar, 'blah')

    def test_doc(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("A foo field.")
            bar = ConfigText("A bar field.")

        self.assertEqual(FooConfig.__doc__, '\n'.join([
            "Test config.",
            "",
            "Configuration options:",
            "",
            ":param foo:",
            "",
            "    A foo field.",
            "",
            ":param str bar:",
            "",
            "    A bar field.",
            ]))

        # And again with the fields defined in a different order to check that
        # we document fields in definition order.
        class BarConfig(Config):
            "Test config."
            bar = ConfigField("A bar field.")
            foo = ConfigField("A foo field.")

        self.assertEqual(BarConfig.__doc__, '\n'.join([
            "Test config.",
            "",
            "Configuration options:",
            "",
            ":param bar:",
            "",
            "    A bar field.",
            "",
            ":param foo:",
            "",
            "    A foo field.",
            ]))

    def test_inheritance(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("From base class.")

        class BarConfig(FooConfig):
            "Another test config."
            bar = ConfigField("New field.")

        conf = BarConfig({'foo': 'blah', 'bar': 'bleh'})
        self.assertEqual(conf.fields,
                         [FooConfig.foo, BarConfig.bar])
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, 'bleh')

        # Inherited fields should come before local fields.
        self.assertEqual(BarConfig.__doc__, '\n'.join([
            "Another test config.",
            "",
            "Configuration options:",
            "",
            ":param foo:",
            "",
            "    From base class.",
            "",
            ":param bar:",
            "",
            "    New field.",
            ]))

    def test_double_inheritance(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("From base class.")

        class BarConfig(FooConfig):
            "Another test config."
            bar = ConfigField("From middle class.")

        class BazConfig(BarConfig):
            "Second-level inheritance test config."
            baz = ConfigField("From top class.")

        conf = BazConfig({'foo': 'blah', 'bar': 'bleh', 'baz': 'blerg'})
        self.assertEqual(conf.fields,
                         [FooConfig.foo, BarConfig.bar, BazConfig.baz])
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, 'bleh')
        self.assertEqual(conf.baz, 'blerg')

    def test_validation(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("foo", required=True, static=True)
            bar = ConfigInt("bar", required=True)

        conf = FooConfig({'foo': 'blah', 'bar': 1})
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, 1)

        self.assertRaises(ConfigError, FooConfig, {})
        self.assertRaises(ConfigError, FooConfig, {'foo': 'blah', 'baz': 'hi'})

    def test_static_validation(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("foo", required=True, static=True)
            bar = ConfigInt("bar", required=True)

        conf = FooConfig({'foo': 'blah'}, static=True)
        self.assertEqual(conf.foo, 'blah')

        conf = FooConfig({'foo': 'blah', 'bar': 'hi'}, static=True)
        self.assertEqual(conf.foo, 'blah')

        self.assertRaises(ConfigError, FooConfig, {}, static=True)

    def test_post_validate(self):
        class FooConfig(Config):
            foo = ConfigInt("foo", required=True)

            def post_validate(self):
                if self.foo < 0:
                    self.raise_config_error("'foo' must be non-negative")

        conf = FooConfig({'foo': 1})
        self.assertEqual(conf.foo, 1)

        self.assertRaises(ConfigError, FooConfig, {'foo': -1})


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

    def test_text_field(self):
        field = self.make_field(ConfigText)
        self.assertEqual('foo', self.field_value(field, 'foo'))
        self.assertEqual(u'foo', self.field_value(field, u'foo'))
        self.assertEqual(u'foo\u1234', self.field_value(field, u'foo\u1234'))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))
        self.assert_field_invalid(field, object())
        self.assert_field_invalid(field, 1)

    def test_regex_field(self):
        field = self.make_field(ConfigRegex)
        value = self.field_value(field, '^v[a-z]m[a-z]$')
        self.assertTrue(value.match('vumi'))
        self.assertFalse(value.match('notvumi'))
        self.assertEqual(None, self.field_value(field, None))

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
            field, 'vumi.tests.test_config.ConfigTest')

    def test_int_field(self):
        field = self.make_field(ConfigInt)
        self.assertEqual(0, self.field_value(field, 0))
        self.assertEqual(1, self.field_value(field, 1))
        self.assertEqual(100, self.field_value(field, "100"))
        self.assertEqual(100, self.field_value(field, u"100"))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))
        self.assert_field_invalid(field, object())
        self.assert_field_invalid(field, 2.3)
        self.assert_field_invalid(field, "foo")
        self.assert_field_invalid(field, u"foo\u1234")

    def test_float_field(self):
        field = self.make_field(ConfigFloat)
        self.assertEqual(0, self.field_value(field, 0))
        self.assertEqual(1, self.field_value(field, 1))
        self.assertEqual(0.5, self.field_value(field, 0.5))
        self.assertEqual(0.5, self.field_value(field, "0.5"))
        self.assertEqual(100, self.field_value(field, "100"))
        self.assertEqual(100, self.field_value(field, u"100"))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))
        self.assert_field_invalid(field, object())
        self.assert_field_invalid(field, "foo")
        self.assert_field_invalid(field, u"foo\u1234")

    def test_bool_field(self):
        field = self.make_field(ConfigBool)
        self.assertEqual(False, self.field_value(field, 0))
        self.assertEqual(True, self.field_value(field, 1))
        self.assertEqual(False, self.field_value(field, "0"))
        self.assertEqual(True, self.field_value(field, "true"))
        self.assertEqual(True, self.field_value(field, "TrUe"))
        self.assertEqual(False, self.field_value(field, u"false"))
        self.assertEqual(False, self.field_value(field, ""))
        self.assertEqual(True, self.field_value(field, True))
        self.assertEqual(False, self.field_value(field, False))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))

    def test_list_field(self):
        field = self.make_field(ConfigList)
        self.assertEqual([], self.field_value(field, []))
        self.assertEqual([], self.field_value(field, ()))
        self.assertEqual([0], self.field_value(field, [0]))
        self.assertEqual(["foo"], self.field_value(field, ["foo"]))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))
        self.assert_field_invalid(field, object())
        self.assert_field_invalid(field, "foo")
        self.assert_field_invalid(field, 123)

    def test_list_field_immutable(self):
        field = self.make_field(ConfigList)
        model = self.fake_model(['fault', 'mine'])
        value = field.get_value(model)
        self.assertEqual(value, ['fault', 'mine'])
        value[1] = 'yours'
        self.assertEqual(field.get_value(model), ['fault', 'mine'])

    def test_dict_field(self):
        field = self.make_field(ConfigDict)
        self.assertEqual({}, self.field_value(field, {}))
        self.assertEqual({'foo': 1}, self.field_value(field, {'foo': 1}))
        self.assertEqual({1: 'foo'}, self.field_value(field, {1: 'foo'}))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))
        self.assert_field_invalid(field, object())
        self.assert_field_invalid(field, "foo")
        self.assert_field_invalid(field, 123)

    def test_dict_field_immutable(self):
        field = self.make_field(ConfigDict)
        model = self.fake_model({'fault': 'mine'})
        value = field.get_value(model)
        self.assertEqual(value, {'fault': 'mine'})
        value['fault'] = 'yours'
        self.assertEqual(field.get_value(model), {'fault': 'mine'})

    def test_url_field(self):
        def assert_url(value,
                       scheme='', netloc='', path='', query='', fragment=''):
            self.assertEqual(value.scheme, scheme)
            self.assertEqual(value.netloc, netloc)
            self.assertEqual(value.path, path)
            self.assertEqual(value.query, query)
            self.assertEqual(value.fragment, fragment)

        field = self.make_field(ConfigUrl)
        assert_url(self.field_value(field, 'foo'), path='foo')
        assert_url(self.field_value(field, u'foo'), path='foo')
        assert_url(self.field_value(field, u'foo\u1234'),
                   path='foo\xe1\x88\xb4')
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))
        self.assert_field_invalid(field, object())
        self.assert_field_invalid(field, 1)

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
            field, config={'host': 'localhost', 'port': 80}),
            TCP4ServerEndpoint, interface='localhost', port=80)

        self.assertEqual(self.field_value(field), None)

        self.assert_field_invalid(field, config={'host': 'localhost'})
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
            field, config={'host': 'localhost', 'port': 80}),
            TCP4ClientEndpoint, host='localhost', port=80)

        self.assertEqual(self.field_value(field), None)

        self.assert_field_invalid(field, config={'port': 80})
        self.assert_field_invalid(field, config={'host': 'localhost'})
        self.assert_field_invalid(field, 'foo')

    def test_client_endpoint_field_required(self):
        field = self.make_field(ConfigClientEndpoint, required=True)
        self.check_endpoint(
            self.field_value(field, 'tcp:127.0.0.1:60'),
            TCP4ClientEndpoint, host='127.0.0.1', port=60)
        self.check_endpoint(self.field_value(
            field, config={'host': 'localhost', 'port': 80}),
            TCP4ClientEndpoint, host='localhost', port=80)

        self.assert_field_invalid(field)

    def test_client_endpoint_field_with_port_fallback(self):
        field = self.make_field(
            ConfigClientEndpoint, port_fallback_default=51)
        self.check_endpoint(
            self.field_value(field, config={'host': 'localhost'}),
            TCP4ClientEndpoint, host='localhost', port=51)
        self.check_endpoint(self.field_value(
            field, config={'host': 'localhost', 'port': 80}),
            TCP4ClientEndpoint, host='localhost', port=80)

        self.assert_field_invalid(field, config={'port': 80})
