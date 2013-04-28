# -*- test-case-name: vumi.tests.test_log -*-

"""
Light wrapper around Twisted's rather unfortunate logging system

We reintroduce the concept of log levels (debug, info, warn, error, etc).
Just because it's hard to decide between different levels doesn't mean they
should be thrown out.

The log level and service PID are now also included in the output.

Another feature is named loggers, which twisted doesn't really support well.

Usage:

  import vumi.log


  log.msg("foo")

  # Or with levels
  log.info("foo")
  log.error("foo")

  # named loggers
  log.name('my-subsystem').info('foo')

  mylog = log.name('my-subsystem')
  mylog.info('foo')

"""

import os
import sys
import logging
import time
from datetime import datetime
from functools import partial

from twisted.python import log
from twisted.python import _utilpy3 as util

from vumi.errors import VumiError

# Dictionary of named loggers
_loggers = {}


class VumiLogObserver(object):
    """
    Basically a reworked twisted.python.log.FileLogObserver so that we can
    format the log output
    """
    timeFormat = None

    level_strings = {
        logging.DEBUG: 'DEBUG',
        logging.INFO: 'INFO',
        logging.WARNING: 'WARNING',
        logging.ERROR: 'ERROR',
        logging.CRITICAL: 'CRITICAL',
    }

    def __init__(self, f):
        self.write = f.write
        self.flush = f.flush

    def getTimezoneOffset(self, when):
        """
        from twisted.python.log.FileLogObserver
        """
        offset = datetime.utcfromtimestamp(when) - datetime.fromtimestamp(when)
        return offset.days * (60 * 60 * 24) + offset.seconds

    def formatTime(self, when):
        """
        from twisted.python.log.FileLogObserver
        """
        if self.timeFormat is not None:
            return time.strftime(self.timeFormat, time.localtime(when))

        tzOffset = -self.getTimezoneOffset(when)
        when = datetime.utcfromtimestamp(when + tzOffset)
        tzHour = abs(int(tzOffset / 60 / 60))
        tzMin = abs(int(tzOffset / 60 % 60))
        if tzOffset < 0:
            tzSign = '-'
        else:
            tzSign = '+'
        return '%d-%02d-%02d %02d:%02d:%02d%s%02d%02d' % (
            when.year, when.month, when.day,
            when.hour, when.minute, when.second,
            tzSign, tzHour, tzMin)

    def emit(self, eventDict):
        text = log.textFromEventDict(eventDict)
        if text is None:
            return

        if 'logLevel' not in eventDict:
            level = 'INFO'
        else:
            level = self.level_strings.get(eventDict['logLevel'], 'INFO')

        timeStr = self.formatTime(eventDict['time'])
        fmtDict = {'system': eventDict['system'],
                   'level': level,
                   'pid': os.getpid(),
                   'text': text.replace("\n", "\n\t")}
        msgStr = log._safeFormat("%(pid)s %(level)s [%(system)s]: %(text)s\n",
                                 fmtDict)

        util.untilConcludes(self.write, timeStr + " " + msgStr)
        util.untilConcludes(self.flush)


def logger():
    """ Our log observer factory """
    return VumiLogObserver(sys.stdout).emit


class VumiLog(object):

    def __init__(self, name):
        self.debug = partial(log.msg, logLevel=logging.DEBUG, system=name)
        self.info = partial(log.msg, logLevel=logging.INFO, system=name)
        self.warning = partial(log.msg, logLevel=logging.WARNING, system=name)
        self.error = partial(log.err, logLevel=logging.ERROR, system=name)
        self.critical = partial(log.err, logLevel=logging.CRITICAL,
                                system=name)
        self.msg = self.info
        self.err = self.error


def name(obj):
    """ Returns a named logger """
    global _loggers
    name = None
    if isinstance(obj, str):
        name = obj
    elif isinstance(obj, object):
        name = obj.__class__.__name__
    else:
        raise VumiError("Can't coerce object into a representative string")
    log = _loggers.get(name, None)
    if log is None:
        _loggers[name] = log = VumiLog(name)
    return log


try:
    anonLog
except NameError:
    anonLog = VumiLog('-')


debug = anonLog.debug
info = anonLog.info
warning = anonLog.warning
error = anonLog.error
critical = anonLog.critical
msg = info
err = error
