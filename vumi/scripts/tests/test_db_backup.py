"""Tests for vumi.scripts.db_backup."""

import json

import yaml
from twisted.trial.unittest import TestCase

from vumi.tests.utils import PersistenceMixin

from vumi.scripts.db_backup import ConfigHolder, Options, vumi_version


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


class DbBackupBaseTestCase(TestCase, PersistenceMixin):
    sync_persistence = True

    def setUp(self):
        self._persist_setUp()
        # Make sure we start fresh.
        self.get_redis_manager()._purge_all()
        self.redis = self.get_redis_manager()

    def tearDown(self):
        return self._persist_tearDown()

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
        return self.get_redis_manager(config)

    def mkdbbackup(self, data=None, raw=False):
        if data is None:
            data = self.DB_BACKUP
        if raw:
            dumps = lambda x: x
        else:
            dumps = json.dumps
        return self.mkfile("\n".join([dumps(x) for x in data]))


class BackupDbCmdTestCase(DbBackupBaseTestCase):
    def test_backup_db(self):
        self.redis.set("foo", 1)
        self.redis.set("bar#bar", 2)
        self.redis.set("bar#baz", "bar")
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
                {'key': 'bar', 'type': 'string', 'value': '2'},
                {'key': 'baz', 'type': 'string', 'value': 'bar'},
            ])

    def check_backup(self, key_prefix, expected):
        db_backup = self.mktemp()
        cfg = self.make_cfg(["backup", self.mkdbconfig(key_prefix), db_backup])
        cfg.run()
        with open(db_backup) as backup:
            self.assertEqual([json.loads(x) for x in backup][1:], expected)

    def test_backup_string(self):
        self.redis.set("bar#s", "foo")
        self.check_backup("bar", [{'key': 's', 'type': 'string',
                                   'value': "foo"}])

    def test_backup_list(self):
        lvalue = ["a", "c", "b"]
        for item in lvalue:
            self.redis.rpush("bar#l", item)
        self.check_backup("bar", [{'key': 'l', 'type': 'list',
                                   'value': lvalue}])

    def test_backup_set(self):
        svalue = set(["a", "c", "b"])
        for item in svalue:
            self.redis.sadd("bar#s", item)
        self.check_backup("bar", [{'key': 's', 'type': 'set',
                                   'value': sorted(svalue)}])

    def test_backup_zset(self):
        zvalue = [['z', 1], ['a', 2], ['c', 3]]
        for item, score in zvalue:
            self.redis.zadd("bar#z", **{item: score})
        self.check_backup("bar", [{'key': 'z', 'type': 'zset',
                                   'value': zvalue}])

    def test_hash_backup(self):
        self.redis.hmset("bar#set", {"foo": "1", "baz": "2"})
        self.check_backup("bar", [{'key': 'set', 'type': 'hash',
                                   'value': {"foo": "1", "baz": "2"}}])


class RestoreDbCmdTestCase(DbBackupBaseTestCase):

    DB_BACKUP = [
        {'backup_type': 'redis'},
        {'key': 'bar', 'type': 'string', 'value': "2"},
        {'key': 'baz', 'type': 'string', 'value': "bar"},
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
        redis_data = sorted((k, self.redis.get(k)) for k in self.redis.keys())
        expected_data = [tuple(x.items()[0]) for x in self.RESTORED_DATA]
        expected_data = [("bar#%s" % k, v) for k, v in expected_data]
        self.assertEqual(redis_data, expected_data)

    def check_restore(self, backup_data, restored_data, redis_get):
        backup_data = [{'backup_type': 'redis'}] + backup_data
        cfg = self.make_cfg(["restore", self.mkdbconfig("bar"),
                             self.mkdbbackup(backup_data)])
        cfg.run()
        redis_data = sorted((k, redis_get(k)) for k in self.redis.keys())
        restored_data = sorted([("bar#%s" % k, v)
                                for k, v in restored_data.items()])
        self.assertEqual(redis_data, restored_data)

    def test_restore_string(self):
        self.check_restore([{'key': 's', 'type': 'string', 'value': 'ping'}],
                           {'s': 'ping'}, self.redis.get)

    def test_restore_list(self):
        lvalue = ['z', 'a', 'c']
        self.check_restore([{'key': 'l', 'type': 'list', 'value': lvalue}],
                           {'l': lvalue},
                           lambda k: self.redis.lrange(k, 0, -1))

    def test_restore_set(self):
        svalue = set(['z', 'a', 'c'])
        self.check_restore([{'key': 's', 'type': 'set',
                             'value': list(svalue)}],
                           {'s': svalue}, self.redis.smembers)

    def test_restore_zset(self):
        def get_zset(k):
            return self.redis.zrange(k, 0, -1, withscores=True)
        zvalue = [('z', 1), ('a', 2), ('c', 3)]
        self.check_restore([{'key': 'z', 'type': 'zset', 'value': zvalue}],
                           {'z': zvalue}, get_zset)

    def test_restore_hash(self):
        hvalue = {'a': 'foo', 'b': 'bing'}
        self.check_restore([{'key': 'h', 'type': 'hash', 'value': hvalue}],
                           {'h': hvalue}, self.redis.hgetall)
