# -*- test-case-name: vumi.transports.apposit.tests.test_apposit -*-

import json
from urllib import urlencode

from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi import log
from vumi.utils import http_request_full
from vumi.config import ConfigDict, ConfigText
from vumi.transports.httprpc import HttpRpcTransport


class AppositTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    """Apposit transport config."""

    credentials = ConfigDict(
        "A dictionary where the `from_addr` is used for the key lookup and "
        "the returned value should be a dictionary containing the "
        "corresponding username, password and service id.",
        required=True, static=True)
    outbound_url = ConfigText(
        "The URL to send outbound messages to.", required=True, static=True)


class AppositTransport(HttpRpcTransport):
    """
    HTTP transport for Apposit's interconnection services.
    """

    ENCODING = 'utf-8'
    CONFIG_CLASS = AppositTransportConfig
    CONTENT_TYPE = 'application/x-www-form-urlencoded'

    # Apposit supports multiple channel types (e.g. sms, ussd, ivr, email).
    # Currently, we only have this working for sms, but theoretically, this
    # transport could support other channel types that have corresponding vumi
    # transport types. However, supporting other channels may require a bit
    # more work if they work too differently to the sms channel. For example,
    # support for Apposit's ussd channel will probably require session
    # management, which currently isn't included, since the sms channel does
    # not need this.
    CHANNEL_LOOKUP = {'sms': 'SMS'}
    TRANSPORT_TYPE_LOOKUP = dict(
        reversed(i) for i in CHANNEL_LOOKUP.iteritems())

    EXPECTED_FIELDS = frozenset(['from', 'to', 'channel', 'content', 'isTest'])

    KNOWN_ERROR_RESPONSE_CODES = {
        '102001': "Username Not Set",
        '102002': "Password Not Set",
        '102003': "Username or password is invalid or not authorized",
        '102004': "Service ID Not Set",
        '102005': "Invalid Service Id",
        '102006': "Service Not Found",
        '102007': "Content not set",
        '102008': "To Address Not Set",
        '102009': "From Address Not Set",
        '102010': "Channel Not Set",
        '102011': "Invalid Channel",
        '102012': "The address provided is not subscribed",
        '102013': "The message content id is unregistered or not approved",
        '102014': "Message Content Public ID and Message Content Set",
        '102015': "Message Content Public ID or Message Content Not Set",
        '102022': "One or more messages failed while sending",
        '102024': "Outbound message routing not allowed for service",
        '102025': "Content or Content ID is not Approved",
        '102999': "Other Runtime Error",
    }

    UNKNOWN_RESPONSE_CODE_ERROR = "Response with unknown code received: %s"
    UNSUPPORTED_TRANSPORT_TYPE_ERROR = (
        "No corresponding channel exists for transport type: %s")

    def validate_config(self):
        config = self.get_static_config()
        self.credentials = config.credentials
        self.outbound_url = config.outbound_url
        return super(AppositTransport, self).validate_config()

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(request, self.EXPECTED_FIELDS)

        channel = values.get('channel')
        if channel is not None and channel not in self.CHANNEL_LOOKUP.values():
            errors['unsupported_channel'] = channel

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors),
                                      code=http.BAD_REQUEST)
            return

        self.emit("AppositTransport receiving inbound message from "
                  "%(from)s to %(to)s" % values)

        yield self.publish_message(
            transport_name=self.transport_name,
            message_id=message_id,
            content=values['content'],
            from_addr=values['from'],
            to_addr=values['to'],
            provider='apposit',
            transport_type=self.TRANSPORT_TYPE_LOOKUP[channel],
            transport_metadata={'apposit': {'isTest': values['isTest']}})

        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))

    @inlineCallbacks
    def handle_outbound_message(self, message):
        channel = self.CHANNEL_LOOKUP.get(message['transport_type'])
        if channel is None:
            reason = (self.UNSUPPORTED_TRANSPORT_TYPE_ERROR
                      % message['transport_type'])
            log.msg(reason)
            yield self.publish_nack(message['message_id'], reason)
            return

        self.emit("Sending outbound message: %s" % (message,))

        # build the params dict and ensure each param encoded correctly
        credentials = self.credentials.get(message['from_addr'], {})
        params = dict((k, v.encode(self.ENCODING)) for k, v in {
            'username': credentials.get('username', ''),
            'password': credentials.get('password', ''),
            'serviceId': credentials.get('service_id', ''),
            'fromAddress': message['from_addr'],
            'toAddress': message['to_addr'],
            'content': message['content'],
            'channel': channel,
        }.iteritems())

        self.emit("Making HTTP POST request: %s with body %s" %
                  (self.outbound_url, params))

        response = yield http_request_full(
            self.outbound_url,
            data=urlencode(params),
            method='POST',
            headers={'Content-Type': self.CONTENT_TYPE})

        self.emit("Response: (%s) %r" %
                  (response.code, response.delivered_body))

        response_content = response.delivered_body.strip()
        if response.code == http.OK:
            yield self.publish_ack(user_message_id=message['message_id'],
                                   sent_message_id=message['message_id'])
        else:
            error = self.KNOWN_ERROR_RESPONSE_CODES.get(response_content)
            if error is not None:
                reason = "(%s) %s" % (response_content, error)
            else:
                reason = self.UNKNOWN_RESPONSE_CODE_ERROR % response_content
            log.msg(reason)
            yield self.publish_nack(message['message_id'], reason)
