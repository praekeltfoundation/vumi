# -*- test-case-name: vumi.scripts.tests.test_db_backup -*-
import sys
import json

import yaml
from twisted.python import usage

from vumi.persist.redis_manager import RedisManager


class BackupDbsCmd(usage.Options):

    synopsis = "<db-config.yaml> <db-backup-output.json>"

    optFlags = [
        ["not-sorted", None, "Don't sort keys when doing backup."],
    ]

    def parseArgs(self, db_config, db_backup):
        self.db_config = yaml.safe_load(open(db_config))
        self.db_backup = open(db_backup, "wb")
        self.redis_config = self.db_config.get('redis_manager', {})

    def run(self, cfg):
        cfg.emit("Backing up dbs ...")
        redis = cfg.get_redis(self.redis_config)
        keys = redis.keys()
        if not self.opts['not-sorted']:
            keys = sorted(keys)
        # TODO: dump header?
        for key in keys:
            self.db_backup.write(json.dumps({key: redis.get(key)}))
            self.db_backup.write("\n")
        self.db_backup.close()
        cfg.emit("Backed up %d keys." % (len(keys),))


class RestoreDbsCmd(usage.Options):

    synopsis = "<db-backup.json>"

    def parseArgs(self, db_backup):
        self.db_backup = open(db_backup, "rb")

    def run(self, cfg):
        cfg.emit("Restoring dbs ...")


class Options(usage.Options):
    subCommands = [
        ["backup", None, BackupDbsCmd,
         "Backup databases."],
        ["restore", None, RestoreDbsCmd,
         "Restore databases."],
    ]

    longdesc = """Back-up and restore utility for Vumi
                  Redis (and maybe later Riak) data stores."""

    def postOptions(self):
        if self.subCommand is None:
            raise usage.UsageError("Please specify a sub-command.")


class ConfigHolder(object):
    def __init__(self, options):
        self.options = options

    def emit(self, s):
        print s

    def get_redis(self, config):
        return RedisManager.from_config(config)

    def run(self):
        self.options.subOptions.run(self)


if __name__ == '__main__':
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    cfg = ConfigHolder(options)
    cfg.run()
