"""Tests for vumi.scripts.vumi_redis_tools."""

import yaml

from vumi.scripts.vumi_redis_tools import (
    ConfigHolder, Options, Task, TaskError, Count)
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


class DummyTask(Task):
    name = "dummy"

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


class CountTestCase(VumiTestCase):
    def test_name(self):
        t = Count()
        self.assertEqual(t.name, "count")

    def test_create(self):
        t = Task.parse("count")
        self.assertEqual(t.name, "count")
        self.assertEqual(type(t), Count)

    # TODO: add tests for setup, apply, teardown


class OptionsTestCase(VumiTestCase):
    pass  # TODO: implement tests


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
        cfg = self.make_cfg(["-t", "count", self.mk_redis_config("bar"), "*"])
        cfg.run()
        self.assertEqual(cfg.output, [
            # TODO: add output
        ])

    def test_multiple_task(self):
        pass  # TODO: implement test
