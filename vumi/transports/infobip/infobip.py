# -*- coding: utf-8 -*-
# -*- test-case-name: vumi.transports.infobip.tests.test_infobip -*-

"""Infobip USSD transport."""

import json

import redis
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport
from vumi.transports.failures import PermanentFailure
from vumi.utils import get_deploy_int


class InfobipTransport(HttpRpcTransport):
    """Infobip transport.

    Currently only supports the Infobip USSD interface.

    Configuration parameters:

    :type ussd_session_timeout: int
    :param ussd_session_timeout:
        Number of seconds before USSD session information stored in
        Redis expires. Default is 600s.

    Excerpt from :title-reference:`INFOBIP USSD Gateway to Third-party
    Application HTTP/REST/JSON Web Service API`:

    Third party application provides four methods for session
    management. Their parameters are as follows:

    * sessionActive (type Boolean) – true if the session is active,
      false otherwise. The parameter is mandatory.

    * sessionId (type String) – is generated for each started session
      and. The parameter is mandatory.  exitCode (type Integer) –
      defined the status of the session that is complete. All the exit
      codes can be found in Table 1. The parameter is mandatory.

    * reason (type String) – in case a third-party applications
      releases the session before its completion it will contain the
      reason for the release. The parameter is used for logging
      purposes and is mandatory.  msisdn (type String) – of the user
      that sent the response to the menu request. The parameter is
      mandatory.

    * imsi (type String) – of the user that sent the response to the
      menu request. The parameter is optional.

    * text (type String) – text the user entered in the response. The
      parameter is mandatory.  shortCode – Short code entered in the
      mobile initiated session or by calling start method. The
      parameter is optional.

    * language (type String)– defines which language will be used for
      message text. Used in applications that support
      internationalization. The parameter should be set to null if not
      used. The parameter is optional.

    * optional (type String)– left for future usage scenarios. The
      parameter is optional.  ussdGwId (type String)– id of the USSD
      GW calling the third-party application. This parameter is
      optional.

    Responses to requests sent from the third-party-applications have
    the following parameters:

    * ussdMenu (type String)– menu to send as text to the user. The
      parameter is mandatory.

    * shouldClose (type boolean)– set to true if this is the last
      message in this session sent to the user, false if there will be
      more. The parameter is mandatory. Please note that the first
      message in the session will always be sent as a menu
      (i.e. shouldClose will be set to false).

    * thirdPartyId (type String)– Id of the third-party application or
      server. This parameter is optional.

    * responseExitCode (type Integer) – request processing exit
      code. Parameter is mandatory.
    """

    # method_name -> session_event, handler_name, sends_json
    METHOD_TO_HANDLER = {
        "status": (TransportUserMessage.SESSION_NONE,
                   "handle_infobip_status", False),
        "start": (TransportUserMessage.SESSION_NEW,
                  "handle_infobip_start", True),
        "response": (TransportUserMessage.SESSION_RESUME,
                     "handle_infobip_response", True),
        "end": (TransportUserMessage.SESSION_CLOSE,
                "handle_infobip_end", True),
        }

    @inlineCallbacks
    def setup_transport(self):
        yield super(InfobipTransport, self).setup_transport()
        self.r_server = redis.Redis("localhost",
                                    db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = "infobip:%s" % (self.transport_name,)
        log.msg("r_prefix = %s" % self.r_prefix)
        self.r_session_timeout = int(self.config.get("ussd_session_timeout",
                                                     600))

    def r_key(self, session_id):
        return ":".join([self.r_prefix, str(session_id)])

    def save_ussd_params(self, session_id, params):
        skey = self.r_key(session_id)
        self.r_server.delete(skey)
        for s_key, s_value in params.items():
            self.r_server.hset(skey, s_key, s_value)
        self.r_server.expire(skey, self.r_session_timeout)

    def get_ussd_params(self, session_id):
        skey = self.r_key(session_id)
        return self.r_server.hgetall(skey)

    def clear_ussd_params(self, session_id):
        skey = self.r_key(session_id)
        self.r_server.delete(skey)

    def send_error(self, msgid, reason, code=400):
        response_data = {
            "responseExitCode": code,
            "responseMessage": reason,
            }
        self.finish_request(msgid, json.dumps(response_data))

    def handle_infobip_status(self, msgid, session_id, eq_data):
        params = self.get_ussd_params(session_id)
        response_data = {
            "sessionActive": bool(params),
            "responseExitCode": 200,
            "responseMessage": "",
            }
        self.finish_request(msgid, json.dumps(response_data))
        return None

    def handle_infobip_start(self, msgid, session_id, req_data):
        message_dict = self.get_ussd_params(session_id)
        if message_dict:
            self.send_error(msgid,
                            "USSD session %r already started" % (session_id,))
            return None
        try:
            from_addr = req_data["msisdn"]
            content = req_data["text"]
        except KeyError, e:
            self.send_error(msgid, "Missing required JSON field: %r" % (e,))
            return None

        message_dict = {
            "from_addr": from_addr,
            # unfortunately shortCode is not as mandatory as the
            # Infobip documentation claims
            "to_addr": req_data.get("shortCode") or "",
            # ussdGwId isn't documented but it does get sent and
            # contains values like "live2".
            "provider": req_data.get("ussdGwId", ""),
            }
        self.save_ussd_params(session_id, message_dict)
        message_dict["content"] = content
        return message_dict

    def handle_infobip_response(self, msgid, session_id, req_data):
        message_dict = self.get_ussd_params(session_id)
        if not message_dict:
            self.send_error(msgid, "Invalid USSD session %r" % (session_id,))
            return None
        try:
            content = req_data["text"]
        except KeyError, e:
            self.send_error(msgid, "Missing required JSON field: %r" % (e,))
            return None
        message_dict["content"] = content
        return message_dict

    def handle_infobip_end(self, msgid, session_id, req_data):
        message_dict = self.get_ussd_params(session_id)
        if not message_dict:
            self.send_error(msgid, "Invalid USSD session %r" % (session_id,))
            return None

        self.clear_ussd_params(session_id)
        response_data = {"responseExitCode": 200, "responseMessage": ""}
        self.finish_request(msgid, json.dumps(response_data))
        message_dict["content"] = None
        return message_dict

    def handle_infobip_error(self, msgid, session_id, req_data):
        self.send_error(msgid, req_data.get("error", "Invalid request"))
        return None

    def handle_raw_inbound_message(self, msgid, request):
        parts = request.path.split('/')
        session_id = parts[-2]
        session_method = parts[-1]
        session_event, session_handler_name, sends_json = (
            self.METHOD_TO_HANDLER.get(session_method,
                                       (TransportUserMessage.SESSION_NONE,
                                       "handle_infobip_error", False)))
        session_handler = getattr(self, session_handler_name)
        req_content = request.content.read()
        log.msg("Incoming message: %r" % (req_content,))
        if sends_json:
            try:
                req_data = json.loads(req_content)
            except ValueError:
                # send bad JSON to error handler
                session_handler = self.handle_infobip_error
                req_data = {"error": "Invalid JSON"}
        else:
            req_data = {}

        message_dict = session_handler(msgid, session_id, req_data)
        if message_dict is not None:
            transport_metadata = {'session_id': session_id}
            message_dict.setdefault("message_id", msgid)
            message_dict.setdefault("session_event", session_event)
            message_dict.setdefault("content", None)
            message_dict["transport_name"] = self.transport_name
            message_dict["transport_type"] = self.config.get('transport_type',
                                                             'ussd')
            message_dict["transport_metadata"] = transport_metadata
            self.publish_message(**message_dict)

    def handle_outbound_message(self, message):
        if message.payload.get('in_reply_to'):
            should_close = (message['session_event']
                            == TransportUserMessage.SESSION_CLOSE)
            response_data = {
                "shouldClose": should_close,
                "ussdMenu": message.get('content'),
                "responseExitCode": 200,
                "responseMessage": "",
                }
            response_id = self.finish_request(message['in_reply_to'],
                                              json.dumps(response_data))
            if response_id is None:
                raise PermanentFailure("Infobip transport could not find"
                                       " original request when attempting"
                                       " to reply.")
            else:
                self.publish_ack(message['message_id'],
                                 sent_message_id=response_id)
        else:
            log.err("Infobip transport cannot process outbound message that"
                    " is not a reply: %r" % message)
            raise PermanentFailure("Infobip transport cannot process outbound"
                                   " message that is not a reply.""")
