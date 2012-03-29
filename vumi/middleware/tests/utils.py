"""Helpers for testing middleware."""

from vumi.middleware.base import BaseMiddleware


class RecordingMiddleware(BaseMiddleware):
    """Marks the calling of middleware in a custom attribute
       on the processed message. Useful for testing middleware
       is being called correctly.
       """

    def _handle(self, direction, message, endpoint):
        record = message.payload.setdefault('record', [])
        record.append((self.name, direction, endpoint))
        return message

    def handle_inbound(self, message, endpoint):
        return self._handle('inbound', message, endpoint)

    def handle_outbound(self, message, endpoint):
        return self._handle('outbound', message, endpoint)

    def handle_event(self, message, endpoint):
        return self._handle('event', message, endpoint)

    def handle_failure(self, message, endpoint):
        return self._handle('failure', message, endpoint)
