# -*- test-case-name: vumi.transports.api.tests.test_api -*-
import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport
from vumi.config import ConfigBool, ConfigList, ConfigDict


class HttpApiConfig(HttpRpcTransport.CONFIG_CLASS):
    "HTTP API configuration."
    reply_expected = ConfigBool(
        "True if a reply message is expected.", default=False, static=True)
    allowed_fields = ConfigList(
        "The list of fields a request is allowed to contain. Defaults to the"
        " DEFAULT_ALLOWED_FIELDS class attribute.", static=True)
    field_defaults = ConfigDict(
        "Default values for fields not sent by the client.",
        default={}, static=True)


class HttpApiTransport(HttpRpcTransport):
    """
    Native HTTP API for getting messages into vumi.

    NOTE: This has no security. Put it behind a firewall or something.

    If reply_expected is True, the transport will wait for a reply message
    and will return the reply's content as the HTTP response body. If False,
    the message_id of the dispatched incoming message will be returned.
    """

    transport_type = 'http_api'

    ENCODING = 'utf-8'
    CONFIG_CLASS = HttpApiConfig
    DEFAULT_ALLOWED_FIELDS = (
        'content',
        'to_addr',
        'from_addr',
        'group',
        'session_event',
        )

    def setup_transport(self):
        config = self.get_static_config()
        self.reply_expected = config.reply_expected
        allowed_fields = config.allowed_fields
        if allowed_fields is None:
            allowed_fields = self.DEFAULT_ALLOWED_FIELDS
        self.allowed_fields = set(allowed_fields)
        self.field_defaults = config.field_defaults
        return super(HttpApiTransport, self).setup_transport()

    def handle_outbound_message(self, message):
        if self.reply_expected:
            return super(HttpApiTransport, self).handle_outbound_message(
                message)
        log.msg("HttpApiTransport dropping outbound message: %s" % (message))

    def get_api_field_values(self, request, required_fields):
        values = self.field_defaults.copy()
        errors = {}
        for field in request.args:
            if field not in self.allowed_fields:
                errors.setdefault('unexpected_parameter', []).append(field)
            else:
                values[field] = (
                    request.args.get(field)[0].decode(self.ENCODING))
        for field in required_fields:
            if field not in values and field in self.allowed_fields:
                errors.setdefault('missing_parameter', []).append(field)
        return values, errors

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_api_field_values(request, ['content',
                                                    'to_addr', 'from_addr'])
        if errors:
            yield self.finish_request(message_id, json.dumps(errors), code=400)
            return
        log.msg(('HttpApiTransport sending from %(from_addr)s to %(to_addr)s '
                 'message "%(content)s"') % values)
        payload = {
            'message_id': message_id,
            'transport_type': self.transport_type,
            }
        payload.update(values)
        yield self.publish_message(**payload)
        if not self.reply_expected:
            yield self.finish_request(message_id,
                                      json.dumps({'message_id': message_id}))
