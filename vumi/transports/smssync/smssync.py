# -*- test-case-name: vumi.transports.smssync.tests.test_smssync -*-
import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport


class SmsSyncTransport(HttpRpcTransport):
    """
    Ushandi SMSSync Transport for getting messages into vumi.

    web_path : str
        The path relative to the host where this listens
    web_port : int
        The port this listens on
    transport_name : str
        The name this transport instance will use to create its queues
    secret : str (default '')
        For security, compared against a string entered in the Android app.
    """

    transport_type = 'smssync'

    def setup_transport(self):
        self.secret = self.config.get('secret')
        return super(SmsSyncTransport, self).setup_transport()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        yield self.finish_request(message.payload['in_reply_to'],
                                  self.generate_response(True))

    def generate_response(self, success=False):
        response = {
            'payload': {
                'success': success
            }
        }

        return json.dumps(response)

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        if self.secret != request.args['secret'][0]:
            yield self.finish_request(message_id, self.generate_response(False))
            return

        message = {
            'message_id': message_id,
            'transport_type': self.transport_type,
            'to_addr': request.args['sent_to'][0],
            'from_addr': request.args['from'][0],
            'content': request.args['message'][0]
        }            

        yield self.publish_message(**message)