# -*- test-case-name: vumi.scripts.tests.test_vumi_redis_tools -*-
import sys

import yaml
from twisted.python import usage

from vumi.persist.redis_manager import RedisManager


class Task(object):
    def __init__(self):
        pass

    def setup(self):
        pass

    def teardown(self):
        pass

    def apply(self, key):
        pass


class Options(usage.Options):

    synopsis = "<config-file.yaml> <match-pattern> [-t <task> ...]"

    longdesc = """Perform operation on Redis keys."""

    def __init__(self):
        usage.Options.__init__(self)
        self['tasks'] = []

    def parseArgs(self, config_file, match_pattern):
        self['config'] = yaml.safe_load(open(config_file))
        self['match_pattern'] = match_pattern

    def opt_task(self, task):
        self['tasks'].append()

    opt_t = opt_task

    def postOptions(self):
        if self.subCommand is None:
            raise usage.UsageError("Please specify a sub-command.")


class ConfigHolder(object):
    def __init__(self, options):
        self.options = options
        self.config = options['config']
        self.match_pattern = options['match_pattern']
        self.tasks = options['tasks']

    def emit(self, s):
        """
        Print the given string and then a newline.
        """
        print s

    def get_redis(self):
        """
        Create and return a redis manager.
        """
        redis_config = self.config.get('redis_manager', {})
        return RedisManager.from_config(redis_config)

    def run(self):
        """
        Apply all tasks to all keys.
        """
        for task in self.tasks:
            task.setup()

        cursor = None
        redis = self.get_redis()
        while True:
            cursor, keys = redis.scan(cursor, match=self.match_pattern)
            for key in keys:
                for task in self.tasks:
                    key = task.apply(key)
                    if key is None:
                        break
            if cursor is None:
                break

        for task in self.tasks:
            task.teardown()


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
