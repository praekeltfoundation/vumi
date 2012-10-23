# -*- test-case-name: vumi.tests.test_sentry -*-

import logging

from twisted.python import log
from twisted.web.client import HTTPClientFactory, _makeGetterFactory
from twisted.internet.defer import DeferredList


DEFAULT_LOG_CONTEXT_SENTINEL = "_SENTRY_CONTEXT_"


class QuietHTTPClientFactory(HTTPClientFactory):
    """HTTP client factory that doesn't log starting and stopping."""
    noisy = False


def quiet_get_page(url, contextFactory=None, *args, **kwargs):
    """A version of getPage that uses QuietHTTPClientFactory."""
    return _makeGetterFactory(
        url,
        QuietHTTPClientFactory,
        contextFactory=contextFactory,
        *args, **kwargs).deferred


def vumi_raven_client(dsn, log_context_sentinel=None):
    import raven
    from raven.transport.base import TwistedHTTPTransport
    from raven.transport.registry import TransportRegistry

    remaining_deferreds = set()
    if log_context_sentinel is None:
        log_context_sentinel = DEFAULT_LOG_CONTEXT_SENTINEL
    log_context = {log_context_sentinel: True}

    class VumiRavenHTTPTransport(TwistedHTTPTransport):

        scheme = ['http', 'https']

        def _get_page(self, data, headers):
            d = quiet_get_page(self._url, method='POST', postdata=data,
                               headers=headers)
            self._track_deferred(d)
            return d

        def _track_deferred(self, d):
            remaining_deferreds.add(d)
            d.addBoth(self._untrack_deferred, d)

        def _untrack_deferred(self, d, result):
            remaining_deferreds.discard(d)
            return result

        def send(self, data, headers):
            d = self._get_page(data, headers)
            d.addErrback(lambda f: log.err(f, **log_context))

    class VumiRavenClient(raven.Client):

        _registry = TransportRegistry(transports=[
            VumiRavenHTTPTransport
        ])

        def teardown(self):
            return DeferredList(remaining_deferreds)

    return VumiRavenClient(dsn)


class SentryLogObserver(object):
    """Twisted log observer that logs to a Raven Sentry client."""

    DEFAULT_ERROR_LEVEL = logging.ERROR
    DEFAULT_LOG_LEVEL = logging.INFO

    def __init__(self, client, log_context_sentinel=None):
        if log_context_sentinel is None:
            log_context_sentinel = DEFAULT_LOG_CONTEXT_SENTINEL
        self.client = client
        self.log_context_sentinel = log_context_sentinel
        self.log_context = {self.log_context_sentinel: True}

    def level_for_event(self, event):
        level = event.get('logLevel')
        if level is not None:
            return level
        if event.get('isError'):
            level = self.DEFAULT_ERROR_LEVEL
        else:
            level = self.DEFAULT_LOG_LEVEL

    def logger_for_event(self, event):
        return event.get('system', 'unknown')

    def _log_to_sentry(self, event):
        data = {
            "logger": self.logger_for_event(event),
            "level": self.level_for_event(event),
        }
        failure = event.get('failure')
        if failure:
            exc_info = (failure.type, failure.value, failure.tb)
            self.client.captureException(exc_info, data=data)
        elif event['message']:
            for msg in event['message']:
                self.client.captureMessage(msg, data=data)
        else:
            self.client.captureMessage("No message.", data=data)

    def __call__(self, event):
        if self.log_context_sentinel in event:
            return
        log.callWithContext(self.log_context, self._log_to_sentry, event)


def setup_sentry(dsn):
    client = vumi_raven_client(dsn=dsn)
    sentry_log_observer = SentryLogObserver(client)

    log.theLogPublisher.addObserver(sentry_log_observer)

    def remove_sentry():
        log.theLogPublisher.removeObserver(sentry_log_observer)

    return remove_sentry
