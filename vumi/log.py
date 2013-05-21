# -*- test-case-name: vumi.tests.test_log -*-

"""
A light wrapper around Twisted's logging system, to work around some issues,
while still preserving compatibility.

We reintroduce the concept of log levels (debug, info, warn, error, etc).
Just because it can be hard to decide between different levels doesn't mean
they should be done away. Having an "ERROR" token in the logs also makes
it easier to grep for actual errors.



The log level and service PID are now also included in the output.

Another feature is named loggers, so that one indicate which module
or class logged a message. Twisted does a really bad job with this.

Usage:

  import vumi.log

  # supports existing twisted.python.log API
  log.msg('foo')
  log.err(exc, "Connection failed")

  # With log levels!
  log.debug('foo')
  log.info('foo')
  log.error('foo')

  # Named loggers!
  log.name('subsystem').info('foo')
  log.name(self).info('foo')

  mylog = log.name('subsystem')
  mylog.info('foo')

  # Built-in formatting
  #
  # This is a good thing as it allows Sentry to rollup/aggregate error events,
  # and also reduces verbiage in the logging statements
  #
  log.error('Connection failed (client_id=%s,seq=%s), id, seq)

  # Exceptions and Failures
  #
  # The logger recognizes error objects and will create useful
  # log messages with them
  #
  # When you want a log an error but also print some useful context, pass
  # the error object using the 'cause' keyword, like below:
  #
  log.error('Connection failed (client_id=%s,token=%s), id, token, cause=exc)
  log.error(cause=exc)
  log.error(exc)

"""

import os
import sys
import logging
import time
from datetime import datetime

from twisted.python import log as _log
from twisted.python import failure

from vumi.errors import VumiError

# Dictionary of named loggers
_loggers = {}


def logger():
    """
    Our log observer factory. Passed to the --logger option for twistd.
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

    def format(event):
        msg = event['message']
        cause = event['cause']
        # Handle cases where we received a cause
        if cause:
            if (not msg) and isinstance(cause, failure.Failure):
                text = ('Unhandled Error: %s\n' + cause.getErrorMessage()
                        + cause.getErrorMessage())
            elif not msg:
                text = repr(cause)
            elif msg:
                text = msg[0] + (' <[cause=%s]>' % cause)
        if msg:
            text = msg
        return text

    def format_compat(event):
        """
        from twisted.python.log.FileLogObserver
        """
        msg = event['message']
        if not msg:
            if event['isError'] and 'failure' in event:
                text = ((event.get('why') or 'Unhandled Error')
                        + '\n' + event['failure'].getTraceback())
            elif 'format' in event:
                text = _log._safeFormat(event['format'], event)
            else:
                # we don't know how to log this
                return
        else:
            text = ' '.join(map(str, msg))
        return text

    def emit(self, event):
        if 'vumi-log-v2' in event:
            text = self.format(event)
        else:
            text = self.format_compat(event)

        if text is None:
            return

        if 'logLevel' not in event:
            level = 'INFO'
        else:
            level = self.level_strings.get(event['logLevel'], 'INFO')

        time_str = self.formatTime(event['time'])
        fmt_dict = {
            'system': event['system'],
            'level': level,
            'pid': os.getpid(),
            'text': text.replace("\n", "\n\t")
        }
        msg = _log._safeFormat("%(pid)s %(level)s [%(system)s]: %(text)s\n",
                               fmt_dict)

        # Don't need to do async writes here.
        # Disk IO is orders of magnitude faster than Network IO, and
        # the failure conditions are quite different.
        self.write(time_str + " " + msg)
        self.flush()


class VumiLog(object):

    def __init__(self, name=None):
        self.name = name

    def setup(self, obj, params, kw):
        kw['vumi-log-v2'] = True
        if self.name is not None:
            kw['system'] = self.name
        if 'cause' in kw and (isinstance(kw['cause'], Exception) or
                              isinstance(kw['cause'], failure.Failure)):
            kw['cause'] = failure.Failure(kw['cause'])
        if isinstance(obj, str):
            # set message format attributes for Sentry
            kw['sentry_message_format'] = obj
            kw['sentry_message_params'] = params
            msg = [obj % params]
        elif isinstance(obj, failure.Failure):
            kw['cause'] = obj
            msg = []
        elif isinstance(obj, Exception):
            kw['cause'] = failure.Failure(obj)
            msg = []
        else:
            msg = [repr(obj)]
        return msg

    def debug(self, obj, *params, **kw):
        message = self.setup(obj, params, kw)
        _log.msg(*message, logLevel=logging.DEBUG, **kw)

    def info(self, obj, *params, **kw):
        message = self.setup(obj, params, kw)
        _log.msg(*message, logLevel=logging.INFO, **kw)

    def warning(self, obj, *params, **kw):
        message = self.setup(obj, params, kw)
        _log.msg(*message, logLevel=logging.WARNING, **kw)

    def error(self, obj, *params, **kw):
        message = self.setup(obj, params, kw)
        _log.msg(*message, logLevel=logging.ERROR, **kw)

    def critical(self, obj, *params, **kw):
        message = self.setup(obj, params, kw)
        _log.msg(*message, logLevel=logging.CRITICAL, **kw)

    #
    # Legacy shims which are 100% API-compatible with twisted's msg() and err()
    #
    # Since these functions don't accept message format parameters,
    # we can't pass the sentry.interfaces.Message structured data
    # to sentry
    #
    def msg(self, *message, **kw):
        _log.msg(*message, logLevel=logging.INFO, system=self.name)

    def err(self, _stuff=None, _why=None, **kw):
        _log.err(_stuff, _why, **kw)


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

# The root log
_root_log = VumiLog()

debug = _root_log.debug
info = _root_log.info
warning = _root_log.warning
error = _root_log.error
critical = _root_log.critical
msg = _root_log.msg
err = _root_log.err
