# -*- test-case-name: vumi.tests.test_sentry -*-

import logging

import raven

from twisted.python import log
from twisted.application.service import Service
from twisted.python import failure

DEFAULT_LOG_CONTEXT_SENTINEL = "_SENTRY_CONTEXT_"


class SentryLogObserver(object):
    """Twisted log observer that logs to a Raven Sentry client."""

    DEFAULT_ERROR_LEVEL = logging.ERROR
    DEFAULT_LOG_LEVEL = logging.INFO
    LOG_LEVEL_THRESHOLD = logging.WARN

    def __init__(self, client, logger_name, worker_id, sentinel=None):
        if sentinel is None:
            sentinel = DEFAULT_LOG_CONTEXT_SENTINEL
        self.client = client
        self.logger_name = logger_name
        self.worker_id = worker_id
        self.log_context_sentinel = sentinel
        self.log_context = {self.log_context_sentinel: True}

    def level_for_event(self, event):
        level = event.get('logLevel')
        if level is not None:
            return level
        if event.get('isError'):
            return self.DEFAULT_ERROR_LEVEL
        return self.DEFAULT_LOG_LEVEL

    def logger_for_event(self, event):
        if 'vumi-log-v2' in event:
            name = event['system']
        else:
            system = event.get('system', 'Root')
            name = ':'.join(system.split(','))
        return name

    # TODO make two separate implementations, one for legacy twisted logger
    # and for the new logger

    def _log_to_sentry(self, event):
        level = self.level_for_event(event)
        if level < self.LOG_LEVEL_THRESHOLD:
            return
        data = {
            "logger": self.logger_for_event(event),
            "level": level,
        }
        tags = {
            "worker-id": self.worker_id,
        }
        if 'sentry_message_format' in event:
            data['sentry.interfaces.Message'] = {
                "message": event['sentry_message_format'],
                "params": event['sentry_message_params'],
        }
        # Clean this up a bit...
        failure_v1 = event.get('failure')
        cause = event.get('cause')
        if cause and not isinstance(cause, failure.Failure):
            cause = None
        flr = cause or failure_v1
        if flr:
            exc_info = (flr.type, flr.value, flr.tb)
            self.client.captureException(exc_info, data=data, tags=tags)
        else:
            msg = log.textFromEventDict(event)
            self.client.captureMessage(msg, data=data, tags=tags)

    def __call__(self, event):
        if self.log_context_sentinel in event:
            return
        log.callWithContext(self.log_context, self._log_to_sentry, event)


class SentryLoggerService(Service):

    def __init__(self, dsn, logger_name, worker_id, logger=None):
        self.setName('Sentry Logger')
        self.dsn = dsn
        self.client = raven.Client(dsn)
        self.sentry_log_observer = SentryLogObserver(self.client,
                                                     logger_name,
                                                     worker_id)
        self.logger = logger if logger is not None else log.theLogPublisher

    def startService(self):
        self.logger.addObserver(self.sentry_log_observer)
        return Service.startService(self)

    def stopService(self):
        if self.running:
            self.logger.removeObserver(self.sentry_log_observer)
        return Service.stopService(self)

    def registered(self):
        return self.sentry_log_observer in self.logger.observers
