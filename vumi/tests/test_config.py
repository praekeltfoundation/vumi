from twisted.trial.unittest import TestCase

from vumi.errors import ConfigError
from vumi.config import (
    Config, ConfigField, ConfigText, ConfigInt, ConfigFloat, ConfigBool,
    ConfigList, ConfigDict)


class ConfigTest(TestCase):
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

        self.assertEqual(FooConfig.__doc__, '\n\n'.join([
            "Test config.",
            " foo: A foo field.",
            " bar (str): A bar field.",
            ]))

        # And again with the fields defined in a different order to check that
        # we document fields in definition order.
        class BarConfig(Config):
            "Test config."
            bar = ConfigField("A bar field.")
            foo = ConfigField("A foo field.")

        self.assertEqual(BarConfig.__doc__, '\n\n'.join([
            "Test config.",
            " bar: A bar field.",
            " foo: A foo field.",
            ]))

    def test_inheritance(self):
        class FooConfig(Config):
            "Test config."
            foo = ConfigField("From base class.")

        class BarConfig(FooConfig):
            "Another test config."
            bar = ConfigField("New field.")

        conf = BarConfig({'foo': 'blah', 'bar': 'bleh'})
        self.assertEqual(conf.foo, 'blah')
        self.assertEqual(conf.bar, 'bleh')

        # Inherited fields should come before local fields.
        self.assertEqual(BarConfig.__doc__, '\n\n'.join([
            "Another test config.",
            " foo: From base class.",
            " bar: New field.",
            ]))


class FakeModel(object):
    def __init__(self, config):
        self._config_data = config


class ConfigFieldTest(TestCase):
    def fake_model(self, *value):
        config = {}
        if value:
            assert len(value) == 1
            config['foo'] = value[0]
        return FakeModel(config)

    def field_value(self, field, *value):
        return field.get_value(self.fake_model(*value))

    def assert_field_valid(self, field, *value):
        field.validate(self.fake_model(*value))

    def assert_field_invalid(self, field, *value):
        self.assertRaises(ConfigError, field.validate, self.fake_model(*value))

    def make_field(self, field_cls):
        field = field_cls("desc")
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
        self.assertEqual(False, self.field_value(field, u"false"))
        self.assertEqual(True, self.field_value(field, True))
        self.assertEqual(False, self.field_value(field, False))
        self.assertEqual(None, self.field_value(field, None))
        self.assertEqual(None, self.field_value(field))

    def test_list_field(self):
        field = self.make_field(ConfigList)
        self.assertEqual([], self.field_value(field, []))
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
