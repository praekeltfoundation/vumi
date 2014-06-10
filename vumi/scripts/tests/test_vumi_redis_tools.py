"""Tests for vumi.scripts.vumi_redis_tools."""

import yaml

from twisted.python.usage import UsageError

from vumi.scripts.vumi_redis_tools import (
    ConfigHolder, Options, Task, TaskError, Count, Expire, ListKeys)
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class TestConfigHolder(ConfigHolder):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        super(TestConfigHolder, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_redis(self):
        redis_config = self.config.get('redis_manager', {})
        return self.testcase.get_sub_redis(redis_config)


class DummyConfigHolder(object):
    def __init__(self):
        self.output = []

    def emit(self, s):
        self.output.append(s)


class DummyTask(Task):
    """Dummy task for testing."""

    name = "dummy"
    hidden = True

    def __init__(self, a=None, b=None):
        self.a = a
        self.b = b


class TaskTestCase(VumiTestCase):
    def test_name(self):
        t = Task()
        self.assertEqual(t.name, None)

    def test_parse_with_args(self):
        t = Task.parse("dummy:a=foo,b=bar")
        self.assertEqual(t.name, "dummy")
        self.assertEqual(t.a, "foo")
        self.assertEqual(t.b, "bar")
        self.assertEqual(type(t), DummyTask)

    def test_parse_without_args(self):
        t = Task.parse("dummy")
        self.assertEqual(t.name, "dummy")
        self.assertEqual(t.a, None)
        self.assertEqual(t.b, None)
        self.assertEqual(type(t), DummyTask)

    def test_parse_no_task(self):
        self.assertRaises(TaskError, Task.parse, "unknown")

    def test_init(self):
        t = Task()
        cfg = object()
        redis = object()
        t.init(cfg, redis)
        self.assertEqual(t.cfg, cfg)
        self.assertEqual(t.redis, redis)


class CountTestCase(VumiTestCase):

    def setUp(self):
        self.cfg = DummyConfigHolder()

    def mk_count(self):
        t = Count()
        t.init(self.cfg, None)
        t.setup()
        return t

    def test_name(self):
        t = Count()
        self.assertEqual(t.name, "count")

    def test_create(self):
        t = Task.parse("count")
        self.assertEqual(t.name, "count")
        self.assertEqual(type(t), Count)

    def test_setup(self):
        t = self.mk_count()
        self.assertEqual(t.count, 0)

    def test_apply(self):
        t = self.mk_count()
        t.apply("foo")
        self.assertEqual(t.count, 1)

    def test_teardown(self):
        t = self.mk_count()
        for i in range(5):
            t.apply(str(i))
        t.teardown()
        self.assertEqual(self.cfg.output, [
            "Found 5 matching keys.",
        ])


class ExpireTestCase(VumiTestCase):

    def setUp(self):
        self.cfg = DummyConfigHolder()
        self.persistence_helper = self.add_helper(
            PersistenceHelper(is_sync=True))
        self.redis = self.persistence_helper.get_redis_manager()
        self.redis._purge_all()  # Make sure we start fresh.

    def mk_expire(self, seconds=10):
        t = Expire(seconds)
        t.init(self.cfg, self.redis)
        t.setup()
        return t

    def test_name(self):
        t = Expire(seconds=20)
        self.assertEqual(t.name, "expire")

    def test_create(self):
        t = Task.parse("expire:seconds=20")
        self.assertEqual(t.name, "expire")
        self.assertEqual(t.seconds, 20)
        self.assertEqual(type(t), Expire)

    def test_apply(self):
        t = self.mk_expire(seconds=10)
        self.redis.set("key1", "bar")
        self.redis.set("key2", "baz")
        t.apply("key1")
        self.assertTrue(
            0 < self.redis.ttl("key1") <= 10)
        self.assertEqual(
            self.redis.ttl("key2"), None)


class ListKeysTestCase(VumiTestCase):

    def setUp(self):
        self.cfg = DummyConfigHolder()

    def mk_list(self):
        t = ListKeys()
        t.init(self.cfg, None)
        t.setup()
        return t

    def test_name(self):
        t = ListKeys()
        self.assertEqual(t.name, "list")

    def test_create(self):
        t = Task.parse("list")
        self.assertEqual(t.name, "list")
        self.assertEqual(type(t), ListKeys)

    def test_apply(self):
        t = self.mk_list()
        t.apply("key1")
        self.assertEqual(self.cfg.output, [
            "key1",
        ])


class OptionsTestCase(VumiTestCase):
    def mk_file(self, data):
        name = self.mktemp()
        with open(name, "wb") as data_file:
            data_file.write(data)
        return name

    def mk_redis_config(self, key_prefix):
        config = {
            'redis_manager': {
                'key_prefix': key_prefix,
            },
        }
        return self.mk_file(yaml.safe_dump(config))

    def mk_opts_raw(self, args):
        opts = Options()
        opts.parseOptions(args)
        return opts

    def mk_opts(self, args):
        config = self.mk_redis_config("foo")
        return self.mk_opts_raw(args + [config, "*"])

    def test_no_config(self):
        self.assertRaisesRegexp(
            UsageError,
            "Wrong number of arguments.",
            self.mk_opts_raw, [])

    def test_no_pattern(self):
        self.assertRaisesRegexp(
            UsageError,
            "Wrong number of arguments.",
            self.mk_opts_raw, ["config.yaml"])

    def test_no_tasks(self):
        self.assertRaisesRegexp(
            UsageError,
            "Please specify a task.",
            self.mk_opts, [])

    def test_one_task(self):
        opts = self.mk_opts(["-t", "count"])
        self.assertEqual(
            [t.name for t in opts["tasks"]],
            ["count"]
        )

    def test_multiple_tasks(self):
        opts = self.mk_opts(["-t", "list", "-t", "count"])
        self.assertEqual(
            [t.name for t in opts["tasks"]],
            ["list", "count"]
        )

    def test_help(self):
        opts = Options()
        lines = opts.getUsage().splitlines()
        self.assertEqual(lines[-4:], [
            "Available tasks:",
            "      --count   A task that counts the number of keys.",
            "      --expire  A task that sets an expiry time on each key.",
            "      --list    A task that prints out each key.",
        ])


class ConfigHolderTestCase(VumiTestCase):
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(is_sync=True))
        self.redis = self.persistence_helper.get_redis_manager()
        self.redis._purge_all()  # Make sure we start fresh.

    def make_cfg(self, args):
        options = Options()
        options.parseOptions(args)
        return TestConfigHolder(self, options)

    def mk_file(self, data):
        name = self.mktemp()
        with open(name, "wb") as data_file:
            data_file.write(data)
        return name

    def mk_redis_config(self, key_prefix):
        config = {
            'redis_manager': {
                'key_prefix': key_prefix,
            },
        }
        return self.mk_file(yaml.safe_dump(config))

    def get_sub_redis(self, config):
        config = config.copy()
        config['FAKE_REDIS'] = self.redis._client
        config['key_prefix'] = self.redis._key(config['key_prefix'])
        return self.persistence_helper.get_redis_manager(config)

    def test_single_task(self):
        cfg = self.make_cfg([
            "-t", "count", self.mk_redis_config("bar"), "*",
        ])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Found 0 matching keys.',
        ])

    def test_multiple_task(self):
        self.redis.set("bar:key1", "k1")
        self.redis.set("bar:key2", "k2")
        cfg = self.make_cfg([
            "-t", "expire:seconds=10",
            "-t", "count",
            self.mk_redis_config("bar"), "*",
        ])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Found 2 matching keys.',
        ])
        self.assertTrue(0 < self.redis.ttl("bar:key1") <= 10)
        self.assertTrue(0 < self.redis.ttl("bar:key2") <= 10)

    def test_match(self):
        self.redis.set("bar:coffee:key1", "k1")
        self.redis.set("bar:tea:key2", "k2")
        cfg = self.make_cfg([
            "-t", "expire:seconds=10",
            "-t", "count",
            self.mk_redis_config("bar"), "coffee:*",
        ])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Found 1 matching keys.',
        ])
        self.assertTrue(0 < self.redis.ttl("bar:coffee:key1") <= 10)
        self.assertEqual(self.redis.ttl("bar:tea:key2"), None)
