"""Tests for vumi.scripts.db_backup."""

import json

import yaml
from twisted.trial.unittest import TestCase

from vumi.tests.utils import PersistenceMixin

from vumi.scripts.db_backup import ConfigHolder, Options


class TestConfigHolder(ConfigHolder):
    def __init__(self, *args, **kwargs):
        self.output = []
        super(TestConfigHolder, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)


def make_cfg(args):
    options = Options()
    options.parseOptions(args)
    return TestConfigHolder(options)


class DbBackupBaseTestCase(TestCase, PersistenceMixin):
    sync_persistence = True

    DB_CONFIG = {
    }

    def setUp(self):
        self._persist_setUp()
        # Make sure we start fresh.
        self.get_redis_manager()._purge_all()

    def tearDown(self):
        return self._persist_tearDown()

    def mkfile(self, data):
        name = self.mktemp()
        with open(name, "wb") as data_file:
            data_file.write(data)
        return name

    def mkdbconfig(self):
        return self.mkfile(yaml.safe_dump(self.DB_CONFIG))

    def mkdbbackup(self, data=None):
        if data is None:
            data = self.DB_BACKUP
        return self.mkfile("\n".join([json.dumps(x) for x in data]))


class BackupDbCmdTestCase(DbBackupBaseTestCase):
    def test_backup_db(self):
        cfg = make_cfg(["backup", self.mkdbconfig()])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Backing up dbs ...',
        ])


class RestoreDbCmdTestCase(DbBackupBaseTestCase):

    DB_BACKUP = [
    ]

    def test_create_pool_range_tags(self):
        cfg = make_cfg(["restore", self.mkdbbackup()])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Restoring dbs ...',
        ])
