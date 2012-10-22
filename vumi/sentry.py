# -*- test-case-name: vumi.tests.test_sentry -*-

from vumi.errors import ConfigError
from vumi.utils import http_request

try:
    from raven.transport import TwistedHttpTransport
    has_raven = True
except ImportError:
    TwistedHttpTransport = object
    has_raven = False


class VumiHttpTransport(TwistedHttpTransport):

    scheme = ['vumi+http', 'vumi+https']

    def send(self, data, headers):
        d = http_request(self._url, data=data, headers=headers,
                         method='POST')
        d.addErrback(lambda f: self.logger.error(
                     'Cannot send error to sentry: %s', f.getTraceback()))


def setup_sentry(dsn):
    if not has_raven:
        raise ConfigError("Setup sentry failed because raven not installed.")
    if not dsn.startswith("vumi+http:"):
        raise ConfigError("Invalid Sentry DSN. It must start with"
                          " 'vumi+http:'.")
    client = raven.Client(dsn=dsn)

    SENTRY_CONTEXT = object()

    def raw_log(event):
        failure = event.get('failure')
        if failure:
            exc_info = (failure.type, failure.value, failure.stack)
            log.callWithContext({SENTRY_CONTEXT: True},
                                client.captureException,
                                exc_info)
        else:
            for msg in event['message']:
                client.captureMessage(msg)
            else:
                client.captureMessage("No message.")

    def log_to_sentry(event):
        if SENTRY_CONTEXT in event:
            return
        log.callWithContext({SENTRY_CONTEXT: True}, raw_log, event)

    log.theLogPublisher.addObserver(log_to_sentry)
