# -*- test-case-name: vumi.scripts.tests.test_db_backup -*-
import sys
import json
import pkg_resources
import traceback
from datetime import datetime

import yaml
from twisted.python import usage

from vumi.persist.redis_manager import RedisManager


def vumi_version():
    vumi = pkg_resources.get_distribution("vumi")
    return str(vumi)


class BackupDbsCmd(usage.Options):

    synopsis = "<db-config.yaml> <db-backup-output.json>"

    optFlags = [
        ["not-sorted", None, "Don't sort keys when doing backup."],
    ]

    def parseArgs(self, db_config, db_backup):
        self.db_config = yaml.safe_load(open(db_config))
        self.db_backup = open(db_backup, "wb")
        self.redis_config = self.db_config.get('redis_manager', {})

    def header(self, cfg):
        return {
            'vumi_version': vumi_version(),
            'format': 'LF separated JSON',
            'backup_type': 'redis',
            'timestamp': cfg.get_utcnow().isoformat(),
            'sorted': not bool(self['not-sorted']),
            'redis_config': self.redis_config,
        }

    def write_line(self, data):
        self.db_backup.write(json.dumps(data))
        self.db_backup.write("\n")

    def run(self, cfg):
        cfg.emit("Backing up dbs ...")
        redis = cfg.get_redis(self.redis_config)
        keys = redis.keys()
        if not self.opts['not-sorted']:
            keys = sorted(keys)
        self.write_line(self.header(cfg))
        for key in keys:
            self.write_line({key: redis.get(key)})
        self.db_backup.close()
        cfg.emit("Backed up %d keys." % (len(keys),))


class RestoreDbsCmd(usage.Options):

    synopsis = "<db-config.yaml> <db-backup.json>"

    def parseArgs(self, db_config, db_backup):
        self.db_config = yaml.safe_load(open(db_config))
        self.db_backup = open(db_backup, "rb")
        self.redis_config = self.db_config.get('redis_manager', {})

    def check_header(self, header):
        if header is None:
            return "Header not found."
        try:
            header = json.loads(header)
        except Exception:
            return "Header not JSON."
        if not isinstance(header, dict):
            return "Header not JSON dict."
        if 'backup_type' not in header:
            return "Header missing backup_type."
        if header['backup_type'] != 'redis':
            return "Only redis backup type currently supported."
        return None

    def run(self, cfg):
        line_iter = iter(self.db_backup)
        try:
            header = line_iter.next()
        except StopIteration:
            header = None

        error = self.check_header(header)
        if error:
            cfg.emit(error)
            cfg.emit("Aborting restore.")
            return

        cfg.emit("Restoring dbs ...")
        redis = cfg.get_redis(self.redis_config)
        keys, skipped = 0, 0
        for i, line in enumerate(line_iter):
            try:
                data = json.loads(line)
            except Exception:
                excinfo = sys.exc_info()
                for s in traceback.format_exception(*excinfo):
                    cfg.emit(s)
                skipped += 1
                continue
            if not isinstance(data, dict) or len(data) != 1:
                cfg.emit("Skipping bad backup line %d:" % (i + 1,))
                skipped += 1
                continue
            key, value = data.items()[0]
            redis.set(key, value)
            keys += 1

        cfg.emit("%d keys successfully restored." % keys)
        if skipped != 0:
            cfg.emit("WARNING: %d bad backup lines skipped." % skipped)


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

    def get_utcnow(self):
        return datetime.utcnow()

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
