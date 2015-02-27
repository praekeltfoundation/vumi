"""Tests for vumi.scripts.vumi_redis_tools."""

import StringIO

import yaml

from twisted.python.usage import UsageError

from vumi.scripts.vumi_redis_tools import (
    scan_keys, TaskRunner, Options, Task, TaskError,
    Count, Expire, Persist, ListKeys, Skip)
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class DummyTaskRunner(object):
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


class TestTask(VumiTestCase):
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
        runner = object()
        redis = object()
        t.init(runner, redis)
        self.assertEqual(t.runner, runner)
        self.assertEqual(t.redis, redis)


class TestCount(VumiTestCase):

    def setUp(self):
        self.runner = DummyTaskRunner()

    def mk_count(self):
        t = Count()
        t.init(self.runner, None)
        t.before()
        return t

    def test_name(self):
        t = Count()
        self.assertEqual(t.name, "count")

    def test_create(self):
        t = Task.parse("count")
        self.assertEqual(t.name, "count")
        self.assertEqual(type(t), Count)

    def test_before(self):
        t = self.mk_count()
        self.assertEqual(t.count, 0)

    def test_process_key(self):
        t = self.mk_count()
        key = t.process_key("foo")
        self.assertEqual(t.count, 1)
        self.assertEqual(key, "foo")

    def test_after(self):
        t = self.mk_count()
        for i in range(5):
            t.process_key(str(i))
        t.after()
        self.assertEqual(self.runner.output, [
            "Found 5 matching keys.",
        ])


class TestExpire(VumiTestCase):

    def setUp(self):
        self.runner = DummyTaskRunner()
        self.persistence_helper = self.add_helper(
            PersistenceHelper(is_sync=True))
        self.redis = self.persistence_helper.get_redis_manager()
        self.redis._purge_all()  # Make sure we start fresh.

    def mk_expire(self, seconds=10):
        t = Expire(seconds)
        t.init(self.runner, self.redis)
        t.before()
        return t

    def test_name(self):
        t = Expire(seconds=20)
        self.assertEqual(t.name, "expire")

    def test_create(self):
        t = Task.parse("expire:seconds=20")
        self.assertEqual(t.name, "expire")
        self.assertEqual(t.seconds, 20)
        self.assertEqual(type(t), Expire)

    def test_process_key(self):
        t = self.mk_expire(seconds=10)
        self.redis.set("key1", "bar")
        self.redis.set("key2", "baz")
        key = t.process_key("key1")
        self.assertEqual(key, "key1")
        self.assertTrue(
            0 < self.redis.ttl("key1") <= 10)
        self.assertEqual(
            self.redis.ttl("key2"), None)


class TestPersist(VumiTestCase):

    def setUp(self):
        self.runner = DummyTaskRunner()
        self.persistence_helper = self.add_helper(
            PersistenceHelper(is_sync=True))
        self.redis = self.persistence_helper.get_redis_manager()
        self.redis._purge_all()  # Make sure we start fresh.

    def mk_persist(self):
        t = Persist()
        t.init(self.runner, self.redis)
        t.before()
        return t

    def test_name(self):
        t = Persist()
        self.assertEqual(t.name, "persist")

    def test_create(self):
        t = Task.parse("persist")
        self.assertEqual(t.name, "persist")
        self.assertEqual(type(t), Persist)

    def test_process_key(self):
        t = self.mk_persist()
        self.redis.setex("key1", 10, "bar")
        self.redis.setex("key2", 20, "baz")
        key = t.process_key("key1")
        self.assertEqual(key, "key1")
        self.assertEqual(
            self.redis.ttl("key1"), None)
        self.assertTrue(
            0 < self.redis.ttl("key2") <= 20)


class TestListKeys(VumiTestCase):

    def setUp(self):
        self.runner = DummyTaskRunner()

    def mk_list(self):
        t = ListKeys()
        t.init(self.runner, None)
        t.before()
        return t

    def test_name(self):
        t = ListKeys()
        self.assertEqual(t.name, "list")

    def test_create(self):
        t = Task.parse("list")
        self.assertEqual(t.name, "list")
        self.assertEqual(type(t), ListKeys)

    def test_process_key(self):
        t = self.mk_list()
        key = t.process_key("key1")
        self.assertEqual(key, "key1")
        self.assertEqual(self.runner.output, [
            "key1",
        ])


class TestSkip(VumiTestCase):

    def setUp(self):
        self.runner = DummyTaskRunner()

    def mk_skip(self, pattern=".*"):
        t = Skip(pattern)
        t.init(self.runner, None)
        t.before()
        return t

    def test_name(self):
        t = Skip(".*")
        self.assertEqual(t.name, "skip")

    def test_create(self):
        t = Task.parse("skip:pattern=.*")
        self.assertEqual(t.name, "skip")
        self.assertEqual(t.regex.pattern, ".*")
        self.assertEqual(type(t), Skip)

    def test_process_key(self):
        t = self.mk_skip("skip_.*")
        self.assertEqual(t.process_key("skip_this"), None)
        self.assertEqual(t.process_key("dont_skip"), "dont_skip")


class TestOptions(VumiTestCase):
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
        exc = self.assertRaises(
            UsageError,
            self.mk_opts_raw, [])
        self.assertEqual(str(exc), "Wrong number of arguments.")

    def test_no_pattern(self):
        exc = self.assertRaises(
            UsageError,
            self.mk_opts_raw, ["config.yaml"])
        self.assertEqual(str(exc), "Wrong number of arguments.")

    def test_no_tasks(self):
        exc = self.assertRaises(
            UsageError,
            self.mk_opts, [])
        self.assertEqual(str(exc), "Please specify a task.")

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
        self.assertEqual(lines[-6:], [
            "Available tasks:",
            "      --count    A task that counts the number of keys.",
            "      --expire   A task that sets an expiry time on each key.",
            "      --list     A task that prints out each key.",
            "      --persist  A task that persists each key.",
            "      --skip     A task that skips keys that match a regular"
            " expression.",
        ])


class TestTaskRunner(VumiTestCase):
    def make_runner(self, tasks, redis=None, pattern="*"):
        if redis is None:
            redis = self.mk_redis_config()
        args = tasks + [redis, pattern]
        options = Options()
        options.parseOptions(args)
        runner = TaskRunner(options)
        runner.redis._purge_all()   # Make sure we start fresh.
        runner.stdout = StringIO.StringIO()
        return runner

    def output(self, runner):
        return runner.stdout.getvalue().splitlines()

    def mk_file(self, data):
        name = self.mktemp()
        with open(name, "wb") as data_file:
            data_file.write(data)
        return name

    def mk_redis_config(self):
        config = {
            'redis_manager': {
                'FAKE_REDIS': True,
            },
        }
        return self.mk_file(yaml.safe_dump(config))

    def test_single_task(self):
        runner = self.make_runner([
            "-t", "count",
        ])
        runner.run()
        self.assertEqual(self.output(runner), [
            'Found 0 matching keys.',
        ])

    def test_multiple_task(self):
        runner = self.make_runner([
            "-t", "expire:seconds=10",
            "-t", "count",
        ])
        runner.redis.set("key1", "k1")
        runner.redis.set("key2", "k2")
        runner.run()
        self.assertEqual(self.output(runner), [
            'Found 2 matching keys.',
        ])
        self.assertTrue(0 < runner.redis.ttl("key1") <= 10)
        self.assertTrue(0 < runner.redis.ttl("key2") <= 10)

    def test_match(self):
        runner = self.make_runner([
            "-t", "expire:seconds=10",
            "-t", "count",
        ], pattern="coffee:*")
        runner.redis.set("coffee:key1", "k1")
        runner.redis.set("tea:key2", "k2")
        runner.run()
        self.assertEqual(self.output(runner), [
            'Found 1 matching keys.',
        ])
        self.assertTrue(0 < runner.redis.ttl("coffee:key1") <= 10)
        self.assertEqual(runner.redis.ttl("tea:key2"), None)

    def test_key_skipping(self):
        runner = self.make_runner([
            "-t", "skip:pattern=key1",
            "-t", "list",
        ])
        runner.redis.set("key1", "k1")
        runner.redis.set("key2", "k2")
        runner.run()
        self.assertEqual(self.output(runner), [
            'key2',
        ])


class TestScanKeys(VumiTestCase):
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(is_sync=True))
        self.redis = self.persistence_helper.get_redis_manager()
        self.redis._purge_all()  # Make sure we start fresh.

    def test_no_keys(self):
        keys = list(scan_keys(self.redis, "*"))
        self.assertEqual(keys, [])

    def test_single_scan_loop(self):
        expected_keys = ["key%d" % i for i in range(5)]
        for key in expected_keys:
            self.redis.set(key, "foo")
        keys = sorted(scan_keys(self.redis, "*"))
        self.assertEqual(keys, expected_keys)

    def test_multiple_scan_loops(self):
        expected_keys = ["key%02d" % i for i in range(100)]
        for key in expected_keys:
            self.redis.set(key, "foo")
        keys = sorted(scan_keys(self.redis, "*"))
        self.assertEqual(keys, expected_keys)

    def test_match(self):
        self.redis.set("coffee:latte", "yes")
        self.redis.set("tea:rooibos", "yes")
        keys = list(scan_keys(self.redis, "coffee:*"))
        self.assertEqual(keys, ["coffee:latte"])
