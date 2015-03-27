#!/usr/bin/env python
# -*- test-case-name: vumi.scripts.tests.test_vumi_redis_tools -*-
import re
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
    hidden = False  # set to True to hide from docs
    runner = None
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
    def task_types(cls):
        return cls.__subclasses__()

    @classmethod
    def _parse_task_type(cls, task_type):
        names = dict((t.name, t) for t in cls.task_types())
        if task_type not in names:
            raise TaskError("Unknown task type %r" % (task_type,))
        return names[task_type]

    @classmethod
    def _parse_param_desc(cls, param_desc):
        params = [x.partition('=') for x in param_desc.split(',')]
        params = [(p, v) for p, _sep, v in params]
        return dict(params)

    def init(self, runner, redis):
        self.runner = runner
        self.redis = redis

    def before(self):
        """Run once before the task applied to any keys."""

    def after(self):
        """Run once afer the task has been applied to all keys."""

    def process_key(self, key):
        """Run once for each key.

        May return either the name of the key (if the key should
        be processed by later tasks), the new name of the key (if
        the key was renamed and should be processed by later tasks)
        or ``None`` (if the key has been deleted or should not be
        processed by further tasks).
        """
        return key


class Count(Task):
    """A task that counts the number of keys."""

    name = "count"

    def __init__(self):
        self.count = None

    def before(self):
        self.count = 0

    def after(self):
        self.runner.emit("Found %d matching keys." % (self.count,))

    def process_key(self, key):
        self.count += 1
        return key


class Expire(Task):
    """A task that sets an expiry time on each key."""

    name = "expire"

    def __init__(self, seconds):
        self.seconds = int(seconds)

    def process_key(self, key):
        self.redis.expire(key, self.seconds)
        return key


class Persist(Task):
    """A task that persists each key."""

    name = "persist"

    def process_key(self, key):
        self.redis.persist(key)
        return key


class ListKeys(Task):
    """A task that prints out each key."""

    name = "list"

    def process_key(self, key):
        self.runner.emit(key)
        return key


class Skip(Task):
    """A task that skips keys that match a regular expression."""

    name = "skip"

    def __init__(self, pattern):
        self.regex = re.compile(pattern)

    def process_key(self, key):
        if self.regex.match(key):
            return None
        return key


class Options(usage.Options):

    synopsis = "<config-file.yaml> <match-pattern> [-t <task> ...]"

    longdesc = "Perform tasks on Redis keys."

    def __init__(self):
        usage.Options.__init__(self)
        self['tasks'] = []

    def getUsage(self, width=None):
        doc = usage.Options.getUsage(self, width=width)
        header = "Available tasks:"
        tasks = sorted(Task.task_types(), key=lambda t: t.name)
        tasks_doc = "".join(usage.docMakeChunks([{
            'long': task.name,
            'doc': task.__doc__,
        } for task in tasks if not task.hidden]))
        return "\n".join([doc, header, tasks_doc])

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


class TaskRunner(object):

    stdout = sys.stdout

    def __init__(self, options):
        self.options = options
        self.match_pattern = options['match_pattern']
        self.tasks = options['tasks']
        self.redis = self.get_redis(options['config'])

    def emit(self, s):
        """
        Print the given string and then a newline.
        """
        self.stdout.write(s)
        self.stdout.write("\n")

    def get_redis(self, config):
        """
        Create and return a redis manager.
        """
        redis_config = config.get('redis_manager', {})
        return RedisManager.from_config(redis_config)

    def run(self):
        """
        Apply all tasks to all keys.
        """
        for task in self.tasks:
            task.init(self, self.redis)

        for task in self.tasks:
            task.before()

        for key in scan_keys(self.redis, self.match_pattern):
            for task in self.tasks:
                key = task.process_key(key)
                if key is None:
                    break

        for task in self.tasks:
            task.after()


if __name__ == '__main__':
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    tasks = TaskRunner(options)
    tasks.run()
