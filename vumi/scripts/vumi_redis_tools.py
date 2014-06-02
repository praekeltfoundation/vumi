# -*- test-case-name: vumi.scripts.tests.test_vumi_redis_tools -*-
import sys

import yaml
from twisted.python import usage

from vumi.persist.redis_manager import RedisManager


class TaskError(Exception):
    """An error occurred while using tasks."""


class Task(object):

    name = None

    def __init__(self):
        pass

    @classmethod
    def parse(cls, task_desc):
        """
        Parse a task description into a task.

        Task description format:

          <task-type>[:[<param>=<value>,<param>=<value>]]
        """
        task_type, _, param_desc = task_desc.partition(':')
        task_cls = cls._parse_task_type(task_type)
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

    def setup(self):
        pass

    def teardown(self):
        pass

    def apply(self, key):
        pass


class Count(Task):
    def __init__(self):
        self._count = None

    def setup(self):
        self._count = 0

    def teardown(self):
        pass

    def apply(self, key):
        self._count += 1


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
        task = Task.parse(task_desc)
        self['tasks'].append(task)

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
