# -*- test-case-name: vumi.scripts.tests.test_vumi_redis_tools -*-
import sys

import yaml
from twisted.python import usage

from vumi.persist.redis_manager import RedisManager


class TaskError(Exception):
    """Raised when an error is encoutered while using tasks."""


class Task(object):
    """
    A task to perform on a redis key.
    """

    name = None
    cfg = None
    redis = None

    @classmethod
    def parse(cls, task_desc):
        """
        Parse a string description into a task.

        Task description format::

          <task-type>[:[<param>=<value>[,...]]]
        """
        task_type, _, param_desc = task_desc.partition(':')
        task_cls = cls._parse_task_type(task_type)
        params = {}
        if param_desc:
            params = cls._parse_param_desc(param_desc)
        return task_cls(**params)

    @classmethod
    def _parse_task_type(cls, task_type):
        names = dict((t.name, t) for t in cls.__subclasses__())
        if task_type not in names:
            raise TaskError("Unknown task type %r" % (task_type,))
        return names[task_type]

    @classmethod
    def _parse_param_desc(cls, param_desc):
        params = [x.partition('=') for x in param_desc.split(',')]
        params = [(p, v) for p, _sep, v in params]
        return dict(params)

    def init(self, cfg, redis):
        self.cfg = cfg
        self.redis = redis

    def setup(self):
        pass

    def teardown(self):
        pass

    def apply(self, key):
        pass


class Count(Task):
    """A task that counts the number of keys."""

    name = "count"

    def __init__(self):
        self.count = None

    def setup(self):
        self.count = 0

    def teardown(self):
        self.cfg.emit("Found %d matching keys." % (self.count,))

    def apply(self, key):
        self.count += 1
        return key


class Expire(Task):
    """A task that sets an expiry time on each key."""

    name = "expire"

    def __init__(self, seconds):
        self.seconds = int(seconds)

    def apply(self, key):
        self.redis.expire(key, self.seconds)
        return key


class ListKeys(Task):
    """A task that prints out each key."""

    name = "list"

    def apply(self, key):
        self.cfg.emit(key)
        return key


class Options(usage.Options):

    synopsis = "<config-file.yaml> <match-pattern> [-t <task> ...]"

    longdesc = """Perform operation on Redis keys."""

    def __init__(self):
        usage.Options.__init__(self)
        self['tasks'] = []

    def parseArgs(self, config_file, match_pattern):
        self['config'] = yaml.safe_load(open(config_file))
        self['match_pattern'] = match_pattern

    def opt_task(self, task_desc):
        """A task to perform on all matching keys."""
        task = Task.parse(task_desc)
        self['tasks'].append(task)

    opt_t = opt_task

    def postOptions(self):
        if not self['tasks']:
            raise usage.UsageError("Please specify a task.")


def scan_keys(redis, match):
    """Iterate over matching keys."""
    prev_cursor = None
    while True:
        cursor, keys = redis.scan(prev_cursor, match=match)
        for key in keys:
            yield key
        if cursor is None:
            break
        if cursor == prev_cursor:
            raise TaskError("Redis scan stuck on cursor %r" % (cursor,))
        prev_cursor = cursor


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
        redis = self.get_redis()

        for task in self.tasks:
            task.init(self, redis)

        for task in self.tasks:
            task.setup()

        for key in scan_keys(redis, self.match_pattern):
            for task in self.tasks:
                key = task.apply(key)
                if key is None:
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
