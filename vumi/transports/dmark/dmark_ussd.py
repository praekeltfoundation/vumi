# -*- test-case-name: vumi.transports.dmark.tests.test_dmark_ussd -*-

import json

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.web import http

from vumi import log
from vumi.components.session import SessionManager
from vumi.config import ConfigDict, ConfigInt
from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport


class DmarkUssdTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    """Config for Dmark USSD transport."""

    ussd_session_timeout = ConfigInt(
        "Number of seconds before USSD session information stored in Redis"
        " expires.",
        default=600, static=True)

    redis_manager = ConfigDict(
        "Redis client configuration.", default={}, static=True)


class DmarkUssdTransport(HttpRpcTransport):
    """Dmark USSD transport over HTTP.

    When a USSD message is received, Dmark will make an HTTP GET request to
    the transport with the following query parameters:

    * ``transactionId``: A unique ID for the USSD session (string).
    * ``msisdn``: The phone number that the message was sent from (string).
    * ``ussdServiceCode``: The USSD Service code the request was made to
      (string).
    * ``transactionTime``: The time the USSD request was received at Dmark,
      as a Unix timestamp (UTC).
    * ``ussdRequestString``: The full content of the USSD request(string).
    * ``creationTime``: The time the USSD request was sent, as a Unix
      timestamp (UTC), if available. (This time is given by the mobile
      network, and may not always be reliable.)
    * ``response``: ``"false"`` if this is a new session, ``"true"`` if it is
      not. Currently not used by the transport (it relies on the
      ``transactionId`` being unique instead).

    The transport may respond to this request either using JSON or form-encoded
    data. A successful response must return HTTP status code 200. Any other
    response code is treated as a failure.

    This transport responds with JSON encoded data. The JSON response
    contains the following keys:

    * ``responseString``: The content to be returned to the phone number that
      originated the USSD request.
    * ``action``: Either ``end`` or ``request``. ``end`` signifies that no
      further interaction is expected from the user and the USSD session should
      be closed. ``request`` signifies that further interaction is expected.

    **Example JSON response**:

    .. sourcecode: javascript

       {
         "responseString": "Hello from Vumi!",
         "action": "end"
       }
    """

    CONFIG_CLASS = DmarkUssdTransportConfig

    transport_type = 'ussd'

    ENCODING = 'utf-8'
    EXPECTED_FIELDS = frozenset([
        'transactionId', 'msisdn', 'ussdServiceCode', 'transactionTime',
        'ussdRequestString', 'creationTime', 'response',
    ])

    @inlineCallbacks
    def setup_transport(self):
        yield super(DmarkUssdTransport, self).setup_transport()
        config = self.get_static_config()
        r_prefix = "vumi.transports.dmark_ussd:%s" % self.transport_name
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, r_prefix,
            max_session_length=config.ussd_session_timeout)

    @inlineCallbacks
    def teardown_transport(self):
        yield super(DmarkUssdTransport, self).teardown_transport()
        yield self.session_manager.stop()

    @inlineCallbacks
    def session_event_for_transaction(self, transaction_id):
        # XXX: There is currently no way to detect when the user closes
        #      the session (i.e. TransportUserMessage.SESSION_CLOSE)
        session_id = transaction_id
        session = yield self.session_manager.load_session(transaction_id)
        if session:
            session_event = TransportUserMessage.SESSION_RESUME
            yield self.session_manager.save_session(session_id, session)
        else:
            session_event = TransportUserMessage.SESSION_NEW
            yield self.session_manager.create_session(
                session_id, transaction_id=transaction_id)
        returnValue(session_event)

    @inlineCallbacks
    def handle_raw_inbound_message(self, request_id, request):
        values, errors = self.get_field_values(request, self.EXPECTED_FIELDS)
        if errors:
            log.msg('Unhappy incoming message: %r' % (errors,))
            self.finish_request(
                request_id, json.dumps(errors), code=http.BAD_REQUEST)
            return

        to_addr = values["ussdServiceCode"]
        from_addr = values["msisdn"]
        session_event = yield self.session_event_for_transaction(
            values["transactionId"])

        yield self.publish_message(
            message_id=request_id,
            content=values["ussdRequestString"],
            to_addr=to_addr,
            from_addr=from_addr,
            provider='dmark',
            session_event=session_event,
            transport_type=self.transport_type,
            transport_metadata={
                'dmark_ussd': {
                    'transaction_id': values['transactionId'],
                    'transaction_time': values['transactionTime'],
                    'creation_time': values['creationTime'],
                }
            })

    def handle_outbound_message(self, message):
        self.emit("DmarkUssdTransport consuming %r" % (message,))
        missing_fields = self.ensure_message_values(message,
                            ['in_reply_to', 'content'])
        if missing_fields:
            return self.reject_message(message, missing_fields)

        if message["session_event"] == TransportUserMessage.SESSION_CLOSE:
            action = "end"
        else:
            action = "request"

        response_data = {
            "responseString": message["content"],
            "action": action,
        }

        response_id = self.finish_request(
            message['in_reply_to'], json.dumps(response_data))
        if response_id is not None:
            return self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
        else:
            return self.publish_nack(
                user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason="Could not find original request.")
