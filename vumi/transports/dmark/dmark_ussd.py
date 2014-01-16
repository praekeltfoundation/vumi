
from vumi.transports.httprpc import HttpRpcTransport


class DmarkUssdTransportConfig(HttpRpcTransport.CONFIG_CLASS):
    """Config for Dmark USSD transport."""


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

    def handle_raw_inbound_message(self, msgid, request):
        raise NotImplementedError("Sub-classes should implement"
                                  " handle_raw_inbound_message.")

    def handle_outbound_message(self, message):
        self.emit("DmarkUssdTransport consuming %s" % (message))
        missing_fields = self.ensure_message_values(message,
                            ['in_reply_to', 'content'])
        if missing_fields:
            return self.reject_message(message, missing_fields)
        else:
            self.finish_request(
                    message.payload['in_reply_to'],
                    message.payload['content'].encode('utf-8'))
            return self.publish_ack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'])
