# -*- test-case-name: vumi.middleware.tests.test_logging -*-

from vumi.middleware import BaseMiddleware
from vumi import log


class LoggingMiddleware(BaseMiddleware):
    """Middleware for logging messages published and consumed by
    transports and applications.

    Optional configuration:

    :param string log_level:
        Log level from :mod:`vumi.log` to log inbound and outbound
        messages and events at. Default is `info`.
    :param string failure_log_level:
        Log level from :mod:`vumi.log` to log failure messages at.
        Default is `error`.
    """

    def setup_middleware(self):
        log_level = self.config.get('log_level', 'info')
        self.message_logger = getattr(log, log_level)
        failure_log_level = self.config.get('failure_log_level', 'error')
        self.failure_logger = getattr(log, failure_log_level)

    def _log(self, direction, logger, msg, endpoint):
        logger("Processed %s message for %s: %s" % (direction, endpoint,
                                                    msg.to_json()))
        return msg

    def handle_inbound(self, message, endpoint):
        return self._log("inbound", self.message_logger, message, endpoint)

    def handle_outbound(self, message, endpoint):
        return self._log("outbound", self.message_logger, message, endpoint)

    def handle_event(self, event, endpoint):
        return self._log("event", self.message_logger, event, endpoint)

    def handle_failure(self, failure, endpoint):
        return self._log("failure", self.failure_logger, failure, endpoint)
