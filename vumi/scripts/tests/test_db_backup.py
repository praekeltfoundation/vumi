"""Tests for vumi.scripts.db_backup."""

import json
import datetime

import yaml

from vumi.scripts.db_backup import ConfigHolder, Options, vumi_version
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class TestConfigHolder(ConfigHolder):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        self.utcnow = None
        super(TestConfigHolder, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

    def get_utcnow(self):
        self.utcnow = super(TestConfigHolder, self).get_utcnow()
        return self.utcnow

    def get_redis(self, config):
        return self.testcase.get_sub_redis(config)


class DbBackupBaseTestCase(VumiTestCase):
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(is_sync=True))
        self.redis = self.persistence_helper.get_redis_manager()
        # Make sure we start fresh.
        self.redis._purge_all()

    def make_cfg(self, args):
        options = Options()
        options.parseOptions(args)
        return TestConfigHolder(self, options)

    def mkfile(self, data):
        name = self.mktemp()
        with open(name, "wb") as data_file:
            data_file.write(data)
        return name

    def mkdbconfig(self, key_prefix):
        config = {
            'redis_manager': {
                'key_prefix': key_prefix,
            },
        }
        return self.mkfile(yaml.safe_dump(config))

    def get_sub_redis(self, config):
        config = config.copy()
        config['FAKE_REDIS'] = self.redis._client
        config['key_prefix'] = self.redis._key(config['key_prefix'])
        return self.persistence_helper.get_redis_manager(config)

    def mkdbbackup(self, data=None, raw=False):
        if data is None:
            data = self.DB_BACKUP
        if raw:
            dumps = lambda x: x
        else:
            dumps = json.dumps
        return self.mkfile("\n".join([dumps(x) for x in data]))


class TestBackupDbCmd(DbBackupBaseTestCase):
    def test_backup_db(self):
        self.redis.set("foo", 1)
        self.redis.set("bar:bar", 2)
        self.redis.set("bar:baz", "bar")
        db_backup = self.mktemp()
        cfg = self.make_cfg(["backup", self.mkdbconfig("bar"), db_backup])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Backing up dbs ...',
            'Backed up 2 keys.',
        ])
        with open(db_backup) as backup:
            self.assertEqual([json.loads(x) for x in backup], [
                {"vumi_version": vumi_version(),
                 "format": "LF separated JSON",
                 "backup_type": "redis",
                 "timestamp": cfg.utcnow.isoformat(),
                 "sorted": True,
                 "redis_config": {"key_prefix": "bar"},
                 },
                {'key': 'bar', 'type': 'string', 'value': '2', 'ttl': None},
                {'key': 'baz', 'type': 'string', 'value': 'bar', 'ttl': None},
            ])

    def check_backup(self, key_prefix, expected):
        db_backup = self.mktemp()
        cfg = self.make_cfg(["backup", self.mkdbconfig(key_prefix), db_backup])
        cfg.run()
        with open(db_backup) as backup:
            self.assertEqual([json.loads(x) for x in backup][1:], expected)

    def test_backup_string(self):
        self.redis.set("bar:s", "foo")
        self.check_backup("bar", [{'key': 's', 'type': 'string',
                                   'value': "foo", 'ttl': None}])

    def test_backup_list(self):
        lvalue = ["a", "c", "b"]
        for item in lvalue:
            self.redis.rpush("bar:l", item)
        self.check_backup("bar", [{'key': 'l', 'type': 'list',
                                   'value': lvalue, 'ttl': None}])

    def test_backup_set(self):
        svalue = set(["a", "c", "b"])
        for item in svalue:
            self.redis.sadd("bar:s", item)
        self.check_backup("bar", [{'key': 's', 'type': 'set',
                                   'value': sorted(svalue), 'ttl': None}])

    def test_backup_zset(self):
        zvalue = [['z', 1], ['a', 2], ['c', 3]]
        for item, score in zvalue:
            self.redis.zadd("bar:z", **{item: score})
        self.check_backup("bar", [{'key': 'z', 'type': 'zset',
                                   'value': zvalue, 'ttl': None}])

    def test_hash_backup(self):
        self.redis.hmset("bar:set", {"foo": "1", "baz": "2"})
        self.check_backup("bar", [{'key': 'set', 'type': 'hash',
                                   'value': {"foo": "1", "baz": "2"},
                                   'ttl': None}])

    def test_ttl_backup(self):
        self.redis.set("bar:s", "foo")
        self.redis.expire("bar:s", 30)
        db_backup = self.mktemp()
        cfg = self.make_cfg(["backup", self.mkdbconfig("bar"), db_backup])
        cfg.run()
        with open(db_backup) as backup:
            [record] = [json.loads(x) for x in backup][1:]
            self.assertTrue(0 < record.pop('ttl') <= 30)
            self.assertEqual(record, {'key': 's', 'type': 'string',
                                      'value': "foo"})


class TestRestoreDbCmd(DbBackupBaseTestCase):

    DB_BACKUP = [
        {'backup_type': 'redis',
         'timestamp': '2012-08-21T23:18:52.413504'},
        {'key': 'bar', 'type': 'string', 'value': "2", 'ttl': None},
        {'key': 'baz', 'type': 'string', 'value': "bar", 'ttl': None},
    ]

    RESTORED_DATA = [
        {'bar': '2'},
        {'baz': "bar"},
    ]

    def _bad_header_test(self, data, expected_response, raw=False):
        cfg = self.make_cfg(["restore", self.mkdbconfig("bar"),
                             self.mkdbbackup(data, raw=raw)])
        cfg.run()
        self.assertEqual(cfg.output, expected_response)

    def test_empty_backup(self):
        self._bad_header_test([], [
            'Header not found.',
            'Aborting restore.',
        ])

    def test_header_not_json(self):
        self._bad_header_test(["."], [
            'Header not JSON.',
            'Aborting restore.',
        ], raw=True)

    def test_non_json_dict(self):
        self._bad_header_test(["."], [
            'Header not JSON dict.',
            'Aborting restore.',
        ])

    def test_header_missing_backup_type(self):
        self._bad_header_test([{}], [
            'Header missing backup_type.',
            'Aborting restore.',
        ])

    def test_unsupported_backup_type(self):
        self._bad_header_test([{'backup_type': 'notredis'}], [
            'Only redis backup type currently supported.',
            'Aborting restore.',
        ])

    def test_restore_backup(self):
        cfg = self.make_cfg(["restore", self.mkdbconfig("bar"),
                             self.mkdbbackup()])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Restoring dbs ...',
            '2 keys successfully restored.',
        ])
        redis_data = sorted(
            (key, self.redis.get(key)) for key in self.redis.keys())
        expected_data = [tuple(x.items()[0]) for x in self.RESTORED_DATA]
        expected_data = [("bar:%s" % (k,), v) for k, v in expected_data]
        self.assertEqual(redis_data, expected_data)

    def test_restore_with_purge(self):
        redis = self.redis.sub_manager("bar")
        redis.set("foo", 1)
        cfg = self.make_cfg(["restore", "--purge", self.mkdbconfig("bar"),
                             self.mkdbbackup()])
        cfg.run()
        self.assertEqual(redis.get("foo"), None)

    def check_restore(self, backup_data, restored_data, redis_get,
                      timestamp=None, args=(), key_prefix="bar"):
        if timestamp is None:
            timestamp = datetime.datetime.utcnow()
        backup_data = [{'backup_type': 'redis',
                        'timestamp': timestamp.isoformat(),
                        }] + backup_data
        cfg = self.make_cfg(["restore"] + list(args) +
                            [self.mkdbconfig(key_prefix),
                             self.mkdbbackup(backup_data)])
        cfg.run()
        redis_data = sorted((key, redis_get(key)) for key in self.redis.keys())
        restored_data = sorted([("%s:%s" % (key_prefix, k), v)
                                for k, v in restored_data.items()])
        self.assertEqual(redis_data, restored_data)

    def test_restore_string(self):
        self.check_restore([{'key': 's', 'type': 'string', 'value': 'ping',
                             'ttl': None}],
                           {'s': 'ping'}, self.redis.get)

    def test_restore_list(self):
        lvalue = ['z', 'a', 'c']
        self.check_restore([{'key': 'l', 'type': 'list', 'value': lvalue,
                             'ttl': None}],
                           {'l': lvalue},
                           lambda k: self.redis.lrange(k, 0, -1))

    def test_restore_set(self):
        svalue = set(['z', 'a', 'c'])
        self.check_restore([{'key': 's', 'type': 'set',
                             'value': list(svalue), 'ttl': None}],
                           {'s': svalue}, self.redis.smembers)

    def test_restore_zset(self):
        def get_zset(k):
            return self.redis.zrange(k, 0, -1, withscores=True)
        zvalue = [('z', 1), ('a', 2), ('c', 3)]
        self.check_restore([{'key': 'z', 'type': 'zset', 'value': zvalue,
                             'ttl': None}],
                           {'z': zvalue}, get_zset)

    def test_restore_hash(self):
        hvalue = {'a': 'foo', 'b': 'bing'}
        self.check_restore([{'key': 'h', 'type': 'hash', 'value': hvalue,
                             'ttl': None}],
                           {'h': hvalue}, self.redis.hgetall)

    def test_restore_ttl(self):
        self.check_restore([{'key': 's', 'type': 'string', 'value': 'ping',
                             'ttl': 30}],
                           {'s': 'ping'}, self.redis.get, key_prefix="bar")
        self.assertTrue(0 < self.redis.ttl("bar:s") <= 30)

    def test_restore_ttl_frozen(self):
        yesterday = datetime.datetime.utcnow() - datetime.timedelta(days=1)
        self.check_restore([{'key': 's', 'type': 'string', 'value': 'ping',
                             'ttl': 30}],
                           {'s': 'ping'}, self.redis.get,
                           timestamp=yesterday,
                           args=["--frozen-ttls"], key_prefix="bar")
        self.assertTrue(0 < self.redis.ttl("bar:s") <= 30)


class TestMigrateDbCmd(DbBackupBaseTestCase):

    def mkrules(self, rules):
        config = {
            "rules": rules,
        }
        return self.mkfile(yaml.safe_dump(config))

    def check_rules(self, rules, data, output, expected):
        header = [{"backup_type": "redis"}]
        result_file = self.mkfile("")
        cfg = self.make_cfg(["migrate",
                             self.mkrules(rules),
                             self.mkdbbackup(header + data),
                             result_file])
        cfg.run()
        self.assertEqual(cfg.output, ["Migrating backup ...",
                                      "Summary of changes:"
                                      ] + output)

        result = [json.loads(x) for x in open(result_file)]
        self.assertEqual(result, header + expected)

    def test_single_regex_rename(self):
        self.check_rules([{"type": "rename", "from": r"foo:", "to": r"baz:"}],
                         [{"key": "foo:bar", "value": "foobar"},
                          {"key": "bar:foo", "value": "barfoo"}],
                         ["  2 records processed.",
                          "  1 records altered."],
                         [{"key": "baz:bar", "value": "foobar"},
                          {"key": "bar:foo", "value": "barfoo"}])

    def test_multiple_renames(self):
        self.check_rules([{"type": "rename", "from": r"foo:", "to": r"baz:"},
                          {"type": "rename", "from": r"bar:", "to": r"rab:"}],
                         [{"key": "foo:bar", "value": "foobar"},
                          {"key": "bar:foo", "value": "barfoo"}],
                         ["  2 records processed.",
                          "  2 records altered."],
                         [{"key": "baz:bar", "value": "foobar"},
                          {"key": "rab:foo", "value": "barfoo"}])

    def test_single_drop(self):
        self.check_rules([{"type": "drop", "key": r"foo:"}],
                         [{"key": "foo:bar", "value": "foobar"},
                          {"key": "bar:foo", "value": "barfoo"}],
                         ["  2 records processed.",
                          "  1 records altered."],
                         [{"key": "bar:foo", "value": "barfoo"}])

    def test_multiple_drops(self):
        self.check_rules([{"type": "drop", "key": r"foo:"},
                          {"type": "drop", "key": r"bar:"}],
                         [{"key": "foo:bar", "value": "foobar"},
                          {"key": "bar:foo", "value": "barfoo"}],
                         ["  2 records processed.",
                          "  2 records altered."],
                         [])


class TestAnalyzeCmd(DbBackupBaseTestCase):
    def mkkeysbackup(self, keys):
        records = [{'backup_type': 'redis'}]
        records.extend({'key': k} for k in keys)
        return self.mkdbbackup(records)

    def check_tree(self, keys, output):
        db_backup = self.mkkeysbackup(keys)
        cfg = self.make_cfg(["analyze", db_backup])
        cfg.run()
        self.assertEqual(cfg.output, ["Keys:", "-----"] + output)

    def test_no_keys(self):
        self.check_tree([], [])

    def test_one_key(self):
        self.check_tree(["foo"], ["foo"])

    def test_two_distinct_keys(self):
        self.check_tree(["foo", "bar"], ["bar", "foo"])

    def test_two_keys_that_share_prefix(self):
        self.check_tree(["foo:bar", "foo:baz"], [
            "foo: (2 leaves)",
        ])

    def test_full_tree(self):
        keys = (["foo:%d" % i for i in range(10)] +
                ["foo:bar:%d" % i for i in range(3)] +
                ["bar:%d" % i for i in range(4)])
        self.check_tree(keys, [
            "bar: (4 leaves)",
            "foo: (10 leaves)",
            "  bar: (3 leaves)",
        ])
