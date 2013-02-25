import json
from urllib import urlencode

from twisted.web import http
from twisted.internet.defer import inlineCallbacks

from vumi import log
from vumi.utils import http_request_full
from vumi.config import ConfigDict, ConfigText
from vumi.transports.httprpc import HttpRpcTransport


class AppositSmsTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    """Apposit transport config."""

    transport_name = ConfigText(
        "The name this transport instance will use to create its queues.",
        required=True, static=True)
    credentials = ConfigDict(
        "A dictionary where the `from_addr` is used for the key lookup and "
        "the returned value should be a dictionary containing the "
        "corresponding username, password and service id.",
        required=True, static=True)
    outbound_url = ConfigText(
        "The URL to send outbound messages to.", required=True, static=True)


class AppositSmsTransport(HttpRpcTransport):
    """
    HTTP transport for Apposit SMS.
    """

    transport_type = 'sms'
    ENCODING = 'UTF-8'
    CONFIG_CLASS = AppositSmsTransportConfig
    EXPECTED_FIELDS = set(['fromAddress', 'toAddress', 'channel', 'content'])
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
    UNKNOWN_ERROR_RESPONSE = "Response with unknown code received: %s"

    def validate_config(self):
        config = self.get_static_config()
        self.credentials = config.credentials
        self.outbound_url = config.outbound_url
        self.noisy = config.noisy
        return super(AppositSmsTransport, self).validate_config()

    def log_msg(self, msg, msg_is_noisy=False):
        if not msg_is_noisy or self.noisy:
            log.msg(msg)

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        values, errors = self.get_field_values(request, self.EXPECTED_FIELDS)

        channel = values.get('channel')
        if channel is not None and channel.lower() != self.transport_type:
            errors['invalid_channel'] = channel

        if errors:
            log.msg('Unhappy incoming message: %s' % (errors,))
            yield self.finish_request(message_id, json.dumps(errors),
                                      code=http.BAD_REQUEST)
            return

        self.log_msg("AppositSmsTransport receiving inbound message from "
                     "%(fromAddress)s to %(toAddress)s" % values, True)

        yield self.publish_message(
            transport_name=self.transport_name,
            message_id=message_id,
            content=values['content'],
            from_addr=values['fromAddress'],
            to_addr=values['toAddress'],
            provider='apposit',
            transport_type=self.transport_type,
        )
        yield self.finish_request(
            message_id, json.dumps({'message_id': message_id}))

    @inlineCallbacks
    def handle_outbound_message(self, message):
        credentials = self.credentials.get(message['from_addr'], {})
        username = credentials.get('username', '')
        password = credentials.get('password', '')
        service_id = credentials.get('service_id', '')

        self.log_msg("Sending outbound message: %s" % (message,), True)

        # build the params dict and ensure each param encoded correctly
        params = dict((k, v.encode(self.ENCODING)) for k, v in {
            'username': username,
            'password': password,
            'serviceId': service_id,
            'fromAddress': message['from_addr'],
            'toAddress': message['to_addr'],
            'content': message['content'],
            'channel': self.transport_type,
        }.iteritems())

        url = '%s?%s' % (self.outbound_url, urlencode(params))
        self.log_msg("Making HTTP request: %s" % (url,), True)

        response = yield http_request_full(url, '', method='POST')
        self.log_msg("Response: (%s) %r" % (
            response.code, response.delivered_body), True)

        response_content = response.delivered_body.strip()
        message_id = message['message_id']
        if response.code == http.OK:
            yield self.publish_ack(user_message_id=message_id,
                                   sent_message_id=message_id)
        else:
            reason = self.KNOWN_ERROR_RESPONSE_CODES.get(response_content,
                self.UNKNOWN_ERROR_RESPONSE % response_content)
            self.log_msg(reason)
            yield self.publish_nack(message_id, reason)
