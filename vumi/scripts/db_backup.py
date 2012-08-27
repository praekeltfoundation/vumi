# -*- test-case-name: vumi.scripts.tests.test_db_backup -*-
import sys
import json
import pkg_resources
import traceback
import time
import calendar
import re
import copy
from datetime import datetime

import yaml
from twisted.python import usage

from vumi.persist.redis_manager import RedisManager
from vumi.errors import ConfigError


def vumi_version():
    vumi = pkg_resources.get_distribution("vumi")
    return str(vumi)


class KeyHandler(object):

    REDIS_TYPES = ('string', 'list', 'set', 'zset', 'hash')

    def __init__(self):
        self._get_handlers = dict((ktype, getattr(self, '%s_get' % ktype))
                                  for ktype in self.REDIS_TYPES)
        self._set_handlers = dict((ktype, getattr(self, '%s_set' % ktype))
                                  for ktype in self.REDIS_TYPES)

    def dump_key(self, redis, key):
        # TODO: save TTL
        key_type = redis.type(key)
        record = {
            'type': key_type,
            'key': key,
            'value': self._get_handlers[key_type](redis, key),
            'ttl': redis.ttl(key),
        }
        return record

    def restore_key(self, redis, record, ttl_offset=0):
        key, key_type, ttl = record['key'], record['type'], record['ttl']
        if ttl is not None:
            ttl -= ttl_offset
            if ttl <= 0:
                return
        self._set_handlers[key_type](redis, key, record['value'])
        if ttl is not None:
            redis.expire(key, ttl)

    def record_okay(self, record):
        if not isinstance(record, dict):
            return False
        for key in ('type', 'key', 'value', 'ttl'):
            if key not in record:
                return False
        return True

    def string_get(self, redis, key):
        return redis.get(key)

    def string_set(self, redis, key, value):
        redis.set(key, value)

    def list_get(self, redis, key):
        return redis.lrange(key, 0, -1)

    def list_set(self, redis, key, value):
        for item in value:
            redis.rpush(key, item)

    def set_get(self, redis, key):
        return sorted(redis.smembers(key))

    def set_set(self, redis, key, value):
        for item in value:
            redis.sadd(key, item)

    def zset_get(self, redis, key):
        return redis.zrange(key, 0, -1, withscores=True)

    def zset_set(self, redis, key, value):
        for item, score in value:
            redis.zadd(key, **{item: score})

    def hash_get(self, redis, key):
        return redis.hgetall(key)

    def hash_set(self, redis, key, value):
        redis.hmset(key, value)


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
        key_handler = KeyHandler()
        keys = redis.keys()
        if not self.opts['not-sorted']:
            keys = sorted(keys)
        self.write_line(self.header(cfg))
        for key in keys:
            record = key_handler.dump_key(redis, key)
            self.write_line(record)
        self.db_backup.close()
        cfg.emit("Backed up %d keys." % (len(keys),))


class RestoreDbsCmd(usage.Options):

    synopsis = "<db-config.yaml> <db-backup.json>"

    optFlags = [
        ["purge", None, "Purge all keys from the redis manager before "
                        "restoring."],
        ["frozen-ttls", None, "Restore TTLs of keys to the same value they "
                              "had when the backup was created, disregarding "
                              "how much time has passed since the backup was "
                              "created. The default is adjust TTLs by the "
                              "amount of time that has passed and to expire "
                              "keys whose TTLs are then zero or negative."],
    ]

    def parseArgs(self, db_config, db_backup):
        self.db_config = yaml.safe_load(open(db_config))
        self.db_backup = open(db_backup, "rb")
        self.redis_config = self.db_config.get('redis_manager', {})

    def check_header(self, header):
        if header is None:
            return None, "Header not found."
        try:
            header = json.loads(header)
        except Exception:
            return None, "Header not JSON."
        if not isinstance(header, dict):
            return None, "Header not JSON dict."
        if 'backup_type' not in header:
            return None, "Header missing backup_type."
        if header['backup_type'] != 'redis':
            return None, "Only redis backup type currently supported."
        return header, None

    def seconds_from_now(self, iso_timestamp):
        seconds_timestamp, _dot, _milliseconds = iso_timestamp.partition('.')
        time_of_backup = time.strptime(seconds_timestamp, "%Y-%m-%dT%H:%M:%S")
        return time.time() - calendar.timegm(time_of_backup)

    def run(self, cfg):
        line_iter = iter(self.db_backup)
        try:
            header = line_iter.next()
        except StopIteration:
            header = None

        header, error = self.check_header(header)
        if error is not None:
            cfg.emit(error)
            cfg.emit("Aborting restore.")
            return

        if self.opts['frozen-ttls']:
            ttl_offset = 0
        else:
            ttl_offset = self.seconds_from_now(header['timestamp'])

        cfg.emit("Restoring dbs ...")
        redis = cfg.get_redis(self.redis_config)
        if self.opts['purge']:
            redis._purge_all()
        key_handler = KeyHandler()
        keys, skipped = 0, 0
        for i, line in enumerate(line_iter):
            try:
                record = json.loads(line)
            except Exception:
                excinfo = sys.exc_info()
                for s in traceback.format_exception(*excinfo):
                    cfg.emit(s)
                skipped += 1
                continue
            if not key_handler.record_okay(record):
                cfg.emit("Skipping bad backup record on line %d." % (i + 1,))
                skipped += 1
                continue
            key_handler.restore_key(redis, record, ttl_offset)
            keys += 1

        cfg.emit("%d keys successfully restored." % keys)
        if skipped != 0:
            cfg.emit("WARNING: %d bad backup lines skipped." % skipped)


class MigrateDbsCmd(usage.Options):

    synopsis = ("<migration-config.yaml> <db-backup.json>"
                " <migrated-backup.json>")

    def parseArgs(self, migration_config, db_backup, migrated_backup):
        self.migration_config = yaml.safe_load(open(migration_config))
        self.db_backup = open(db_backup, "rb")
        self.migrated_backup = open(migrated_backup, "wb")

    def postOptions(self):
        self.rules = self.create_rules(self.migration_config)

    def make_rule_rename(self, kw):
        from_regex = re.compile(kw['from'])
        to_template = kw['to']

        def rule(record):
            key = record['key']
            record['key'] = from_regex.sub(to_template, key)
            return True

        return rule

    def create_rules(self, migration_config):
        rules = []
        for rule in migration_config['rules']:
            kw = rule.copy()
            rule_name = kw.pop('type')
            rule_maker = getattr(self, "make_rule_%s" % rule_name, None)
            if rule_maker is None:
                raise ConfigError("Unknown rule type %r" % rule_name)
            rules.append(rule_maker(kw))
        return rules

    def apply_rules(self, record):
        for rule in self.rules:
            done = rule(record)
            if done:
                return

    def write_line(self, record):
        self.migrated_backup.write(json.dumps(record))
        self.migrated_backup.write("\n")

    def run(self, cfg):
        line_iter = iter(self.db_backup)
        try:
            header = line_iter.next()
        except StopIteration:
            cfg.emit("No header in backup.")
            return
        self.write_line(json.loads(header))

        cfg.emit("Migrating backup ...")
        changed, processed = 0, 0
        for data in line_iter:
            record = json.loads(data)
            new_record = copy.deepcopy(record)
            self.apply_rules(new_record)
            if record != new_record:
                changed += 1
            processed += 1
            self.write_line(new_record)
        self.migrated_backup.close()

        cfg.emit("Summary of changes:")
        cfg.emit("  %d records processed." % processed)
        cfg.emit("  %d records altered." % changed)


class Options(usage.Options):
    subCommands = [
        ["backup", None, BackupDbsCmd,
         "Backup databases."],
        ["restore", None, RestoreDbsCmd,
         "Restore databases."],
        ["migrate", None, MigrateDbsCmd,
         "Rename keys in a database backup."],
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
