# -*- test-case-name: vumi.middleware.tests.test_logging -*-

from confmodel.fields import ConfigText

from vumi.middleware import BaseMiddleware
from vumi.middleware.base import BaseMiddlewareConfig
from vumi import log


class LoggingMiddlewareConfig(BaseMiddlewareConfig):
    """
    Configuration class for the logging middleware.
    """
    log_level = ConfigText(
        "Log level from :mod:`vumi.log` to log inbound and outbound messages "
        "and events at", default='info', static=True)
    failure_log_level = ConfigText(
        "Log level from :mod:`vumi.log` to log failure messages at",
        default='error', static=True)


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
    CONFIG_CLASS = LoggingMiddlewareConfig

    def setup_middleware(self):
        log_level = self.config.log_level
        self.message_logger = getattr(log, log_level)
        failure_log_level = self.config.failure_log_level
        self.failure_logger = getattr(log, failure_log_level)

    def _log(self, direction, logger, msg, connector_name):
        logger("Processed %s message for %s: %s" % (
                direction, connector_name, msg.to_json()))
        return msg

    def handle_inbound(self, message, connector_name):
        return self._log(
            "inbound", self.message_logger, message, connector_name)

    def handle_outbound(self, message, connector_name):
        return self._log(
            "outbound", self.message_logger, message, connector_name)

    def handle_event(self, event, connector_name):
        return self._log("event", self.message_logger, event, connector_name)

    def handle_failure(self, failure, connector_name):
        return self._log(
            "failure", self.failure_logger, failure, connector_name)
