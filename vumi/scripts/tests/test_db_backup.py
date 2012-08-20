"""Tests for vumi.scripts.db_backup."""

import json

import yaml
from twisted.trial.unittest import TestCase

from vumi.tests.utils import PersistenceMixin

from vumi.scripts.db_backup import ConfigHolder, Options


class TestConfigHolder(ConfigHolder):
    def __init__(self, testcase, *args, **kwargs):
        self.testcase = testcase
        self.output = []
        super(TestConfigHolder, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)

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

    def mkdbbackup(self, data=None):
        if data is None:
            data = self.DB_BACKUP
        return self.mkfile("\n".join([json.dumps(x) for x in data]))


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
                {"bar": "2"},
                {"baz": "bar"},
            ])


class RestoreDbCmdTestCase(DbBackupBaseTestCase):

    DB_BACKUP = [
    ]

    def test_create_pool_range_tags(self):
        cfg = self.make_cfg(["restore", self.mkdbbackup()])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Restoring dbs ...',
        ])
