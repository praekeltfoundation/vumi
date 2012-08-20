"""Tests for vumi.scripts.db_backup."""

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

    def setUp(self):
        self._persist_setUp()
        # Make sure we start fresh.
        self.get_redis_manager()._purge_all()

    def tearDown(self):
        return self._persist_tearDown()


class BackupDbCmdTestCase(DbBackupBaseTestCase):
    def test_backup_db(self):
        cfg = make_cfg(["backup", "db_config.yaml"])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Backing up dbs ...',
        ])


class RestoreDbCmdTestCase(DbBackupBaseTestCase):
    def test_create_pool_range_tags(self):
        cfg = make_cfg(["restore", "db_backup.json"])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Restoring dbs ...',
        ])
