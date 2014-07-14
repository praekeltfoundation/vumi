"""Helpers for testing middleware."""

from vumi.middleware.base import BaseMiddleware


class RecordingMiddleware(BaseMiddleware):
    """Marks the calling of middleware in a custom attribute
       on the processed message. Useful for testing middleware
       is being called correctly.
       """

    def _handle(self, direction, message, connector_name):
        record = message.payload.setdefault('record', [])
        record.append((self.name, direction, connector_name))
        return message

    def handle_inbound(self, message, connector_name):
        return self._handle('inbound', message, connector_name)

    def handle_outbound(self, message, connector_name):
        return self._handle('outbound', message, connector_name)

    def handle_event(self, message, connector_name):
        return self._handle('event', message, connector_name)

    def handle_failure(self, message, connector_name):
        return self._handle('failure', message, connector_name)
