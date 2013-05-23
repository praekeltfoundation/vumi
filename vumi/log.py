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

    TIME_FORMAT = '%d-%02d-%02d %02d:%02d:%02d%s%02d%02d'

    LEVEL_NAMES = {
        logging.DEBUG: 'DEBUG',
        logging.INFO: 'INFO',
        logging.WARNING: 'WARNING',
        logging.ERROR: 'ERROR',
        logging.CRITICAL: 'CRITICAL',
    }



    def __init__(self, f):
        self.write = f.write
        self.flush = f.flush

    def timezone_offset(self, when):
        """
        Adapted from twisted.python.log.FileLogObserver
        """
        offset = datetime.utcfromtimestamp(when) - datetime.fromtimestamp(when)
        return offset.days * (60 * 60 * 24) + offset.seconds

    def format_time(self, when):
        """
        Adapted from twisted.python.log.FileLogObserver
        """
        tzoffs = -self.timezone_offset(when)
        when = datetime.utcfromtimestamp(when + tzoffs)
        tzhr = abs(int(tzoffs / 60 / 60))
        tzmin = abs(int(tzoffs / 60 % 60))
        if tzoffs < 0:
            tzsign = '-'
        else:
            tzsign = '+'
        return self.TIME_FORMAT % (
            when.year, when.month, when.day,
            when.hour, when.minute, when.second,
            tzsign, tzhr, tzmin)

    def format(self, event):
        msg = event['message']
        cause = event['cause']
        is_error = event['isError']
        if is_error:
            if (not msg) and isinstance(cause, failure.Failure):
                text = ('Unhandled Error: ' + cause.getErrorMessage() + '\n'
                        + cause.getTraceback())
            elif not msg:
                text = repr(cause)
            elif msg and isinstance(cause, failure.Failure):
                text = (msg[0] + ': ' + cause.getErrorMessage() + '\n'
                        + cause.getTraceback())
            else:
                text = msg[0] + (' <[cause=%s]>' % repr(cause))
        elif msg:
            text = msg[0]
        else:
            text = None
        return text

    def format_compat(self, event):
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
            level = self.LEVEL_NAMES.get(event['logLevel'], 'INFO')

        time_str = self.format_time(event['time'])
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
        self.name = name or 'Root'

    def find_failure(self, exc=None):
        """
        Try and get a failure object which is attached to a traceback
        """
        exc_type, exc_value, exc_tb = sys.exc_info()
        if not exc:
            flr = failure.Failure()
        elif exc == exc_value:
            flr = failure.Failure(exc_value, exc_type, exc_tb)
        else:
            flr = failure.Failure(exc)
        return flr

    def setup(self, obj, params, kw, is_error=False):
        """
        Setup various parameters required by the logging backend.

        A bit gnarly due to the multitude of ways in which failure objects
        can be discovered or derived
        """
        msg = ()
        cause = None

        # Make a suitable message tuple based on the object passed
        # to the logger
        #
        # TODO: Unicode
        #
        if isinstance(obj, str):
            # set message format attributes for Sentry
            kw['sentry_message_format'] = obj
            kw['sentry_message_params'] = params
            msg = (_log._safeFormat(obj, params),)
        elif isinstance(obj, Exception):
            cause = self.find_failure(obj)
        elif isinstance(obj, failure.Failure):
            cause = obj
        else:
            msg = (repr(obj),)

        # Override cause if the user explicitly passed it down to the logger
        if 'cause' in kw:
            cause = kw['cause']
            if isinstance(cause, Exception):
                cause = self.find_failure(cause)

        # If we still haven't got a cause and this log message represents
        # an application error, then search for a failure
        if is_error and cause is None:
            cause = self.find_failure()

        kw['vumi-log-v2'] = True
        kw['isError'] = is_error
        kw['system'] = self.name
        kw['cause'] = cause
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
        message = self.setup(obj, params, kw, is_error=True)
        _log.msg(*message, logLevel=logging.ERROR, **kw)

    def critical(self, obj, *params, **kw):
        message = self.setup(obj, params, kw, is_error=True)
        _log.msg(*message, logLevel=logging.CRITICAL, **kw)

    #
    # Legacy shims which are 100% API-compatible with twisted's msg() and err()
    #
    # Since these functions don't accept message format parameters,
    # we can't pass the sentry.interfaces.Message structured data
    # to sentry
    #
    def msg(self, *message, **kw):
        _log.msg(*message, logLevel=logging.INFO)

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
