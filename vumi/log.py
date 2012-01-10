# -*- test-case-name: vumi.tests.test_log -*-
import logging

from twisted.python import log


def _mk_logger(level, callback):
    level = level.upper()
    level_value = getattr(logging, level, None)
    if not level_value:
        raise RuntimeError, '%s is not a valid level' % (level,)
    def logger(msg, **kwargs):
        defaults = {
            'logLevel': level_value
        }
        defaults.update(kwargs)
        callback('%s: %s' % (level, msg), **defaults)
    return logger

debug = _mk_logger('DEBUG', log.msg)
info = _mk_logger('INFO', log.msg)
warning = _mk_logger('WARNING', log.msg)
error = _mk_logger('ERROR', log.err)
critical = _mk_logger('CRITICAL', log.err)
exception = _mk_logger('ERROR', log.err)