# -*- test-case-name: vumi.transports.infobip.tests.test_infobip_ussd -*-
"""Infobip USSD transport."""

import json

from twisted.internet.defer import inlineCallbacks
from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport


class InfobipUssdTransport(HttpRpcTransport):
    """Infobip USSD transport."""

    METHOD_TO_EVENT = {
        "status": TransportUserMessage.SESSION_NONE,
        "start": TransportUserMessage.SESSION_NEW,
        "response": TransportUserMessage.SESSION_RESUME,
        "end": TransportUserMessage.SESSION_CLOSE,
        }

    def handle_raw_inbound_message(self, msgid, request):
        parts = request.path.split('/')
        ussd_session_id = parts[-2]
        session_method = parts[-1]
        session_event = self.METHOD_TO_EVENT.get(session_method,
                                             TransportUserMessage.SESSION_NONE)
        req_data = json.load(request.content)
        msisdn = req_data["msisdn"]
        content = req_data["text"]
        to_addr = req_data["shortCode"]
        provider = req_data.get("unknown", "")  # TODO: fill-in field

        transport_metadata = {'session_id': ussd_session_id}
        self.publish_message(
                message_id=msgid,
                content=content,
                to_addr=to_addr,
                from_addr=msisdn,
                provider=provider,
                session_event=session_event,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata=transport_metadata,
                )

    def handle_outbound_message(self, message):
        if message.payload.get('in_reply_to') and 'content' in message.payload:
            should_close = (message['session_event']
                            == TransportUserMessage.SESSION_CLOSE)
            response_data = {
                "shouldClose": should_close,
                "ussdMenu": message['content'],
                "responseExitCode": 200,
                "responseMessage": "",
                }
            self.finishRequest(
                    message['in_reply_to'],
                    json.dumps(response_data))
