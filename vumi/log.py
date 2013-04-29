# -*- test-case-name: vumi.tests.test_log -*-

"""
Light wrapper around Twisted's rather unfortunate logging system

We reintroduce the concept of log levels (debug, info, warn, error, etc).
Just because it's hard to decide between different levels doesn't mean they
should be thrown out.

The log level and service PID are now also included in the output.

Another feature is named loggers, so that one indicate which module
or class logged a message.

Usage:

  import vumi.log

  # supports normal twisted.python.log syntax
  log.msg("foo")
  log.err("foo")

  # With log levels
  log.debug("foo")
  log.info("foo")
  log.error("foo")

  # Named loggers.
  # The name param can be any object which can be coerced into string)
  log.name('subsystem').info('foo')

  # or alternatively
  mylog = log.name('subsystem')
  mylog.info('foo')

  # New style formatting
  log.info('Window size is %s bytes. With %s ', 16, bar)

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

# flag to enable/disable new-style formatting
# disabled by default to preserve Vumi API compat
_fancy_formatting = False


def logger():
    """
    Our log observer factory. passed to the --logger option for twistd.
    """
    return VumiLogObserver(sys.stdout).emit


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


class VumiLog(object):

    def __init__(self, name):
        self._debug = partial(log.msg, logLevel=logging.DEBUG, system=name)
        self._info = partial(log.msg, logLevel=logging.INFO, system=name)
        self._warning = partial(log.msg, logLevel=logging.WARNING, system=name)
        self._error = partial(log.err, logLevel=logging.ERROR, system=name)
        self._critical = partial(log.err, logLevel=logging.CRITICAL,
                                system=name)

        self._msg = partial(log.msg, logLevel=logging.INFO, system=name)
        self._err = partial(log.err, logLevel=logging.ERROR, system=name)


    def format(self, fmt, params, kw):
        # set message format attributes for Sentry
        kw['sentry_message_format'] = fmt
        kw['sentry_message_params'] = params
        # format
        return fmt % params

    def debug(self, fmt, *params, **kw):
        text = self.format(fmt, params, kw)
        self._debug(text, **kw)

    def info(self, fmt, *params, **kw):
        text = self.format(fmt, params, kw)
        self._info(text, **kw)

    def warning(self, fmt, *params, **kw):
        text = self.format(fmt, params, kw)
        self._warning(text, **kw)

    def error(self, fmt, *params, **kw):
        text = self.format(fmt, params, kw)
        self._error(text, **kw)

    def critical(self, fmt, *params, **kw):
        text = self.format(fmt, params, kw)
        self._critical(text, **kw)

    #
    # Legacy shims which are compatible with the behavior of
    # twisted.python.log.{msg,err}
    #
    # Since these functions don't accept message format parameters,
    # we can't pass the sentry.interfaces.Message structured data
    # to sentry

    def msg(self, *message, **kw):
        self._msg(*message, **kw)

    def err(self, failure, why, **kw):
        self._err(failure, why, **kw)


def name(obj):
    """ Returns a named logger """
    global _loggers
    name = None
    if isinstance(obj, str):
        name = obj
    elif isinstance(obj, type):
        name = obj.__name__
    elif isinstance(obj, object):
        name = obj.__class__.__name__
    else:
        raise VumiError("Can't coerce object into a representative string")
    log = _loggers.get(name, None)
    if log is None:
        _loggers[name] = log = VumiLog(name)
    return log

# The default, anonymous log
_log = VumiLog('-')

debug = _log.debug
info = _log.info
warning = _log.warning
error = _log.error
critical = _log.critical
msg = _log.info
err = _log.error
