# -*- test-case-name: vumi.transports.infobip.tests.test_infobip -*-
# -*- coding: utf-8 -*-

"""Infobip USSD transport."""

import json

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi import log
from vumi.errors import VumiError
from vumi.message import TransportUserMessage
from vumi.transports.httprpc import HttpRpcTransport
from vumi.components.session import SessionManager


class InfobipError(VumiError):
    """Used to log errors specific to the Infobip transport."""


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

    def validate_config(self):
        super(InfobipTransport, self).validate_config()
        self.r_config = self.config.get('redis_manager', {})

    @inlineCallbacks
    def setup_transport(self):
        yield super(InfobipTransport, self).setup_transport()
        r_prefix = "infobip:%s" % (self.transport_name,)
        session_timeout = int(self.config.get("ussd_session_timeout", 600))
        self.session_manager = yield SessionManager.from_redis_config(
            self.r_config, r_prefix, session_timeout)

    @inlineCallbacks
    def teardown_transport(self):
        yield self.session_manager.stop()
        yield super(InfobipTransport, self).teardown_transport()

    def save_ussd_params(self, session_id, params):
        return self.session_manager.create_session(session_id, **params)

    def get_ussd_params(self, session_id):
        return self.session_manager.load_session(session_id)

    def clear_ussd_params(self, session_id):
        return self.session_manager.clear_session(session_id)

    def send_error(self, msgid, reason, code=400):
        response_data = {
            "responseExitCode": code,
            "responseMessage": reason,
            }
        self.finish_request(msgid, json.dumps(response_data))

    @inlineCallbacks
    def handle_infobip_status(self, msgid, session_id, eq_data):
        params = yield self.get_ussd_params(session_id)
        response_data = {
            "sessionActive": bool(params),
            "responseExitCode": 200,
            "responseMessage": "",
            }
        yield self.finish_request(msgid, json.dumps(response_data))

    @inlineCallbacks
    def handle_infobip_start(self, msgid, session_id, req_data):
        message_dict = yield self.get_ussd_params(session_id)
        if message_dict:
            self.send_error(
                msgid, "USSD session %r already started" % (session_id,))
            return
        try:
            from_addr = req_data["msisdn"]
            content = req_data["text"]
        except KeyError, e:
            self.send_error(msgid, "Missing required JSON field: %r" % (e,))
            return

        message_dict = {
            "from_addr": from_addr,
            # unfortunately shortCode is not as mandatory as the
            # Infobip documentation claims
            "to_addr": req_data.get("shortCode") or "",
            # ussdGwId isn't documented but it does get sent and
            # contains values like "live2".
            "provider": req_data.get("ussdGwId", ""),
            }
        yield self.save_ussd_params(session_id, message_dict)
        message_dict["content"] = content
        returnValue(message_dict)

    @inlineCallbacks
    def handle_infobip_response(self, msgid, session_id, req_data):
        message_dict = yield self.get_ussd_params(session_id)
        if not message_dict:
            self.send_error(msgid, "Invalid USSD session %r" % (session_id,))
            return
        try:
            content = req_data["text"]
        except KeyError, e:
            self.send_error(msgid, "Missing required JSON field: %r" % (e,))
            return
        message_dict["content"] = content
        returnValue(message_dict)

    @inlineCallbacks
    def handle_infobip_end(self, msgid, session_id, req_data):
        message_dict = yield self.get_ussd_params(session_id)
        if not message_dict:
            self.send_error(msgid, "Invalid USSD session %r" % (session_id,))
            return

        yield self.clear_ussd_params(session_id)
        response_data = {"responseExitCode": 200, "responseMessage": ""}
        self.finish_request(msgid, json.dumps(response_data))
        message_dict["content"] = None
        returnValue(message_dict)

    def handle_infobip_error(self, msgid, session_id, req_data):
        self.send_error(msgid, req_data.get("error", "Invalid request"))

    @inlineCallbacks
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

        message_dict = yield session_handler(msgid, session_id, req_data)
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
                err_msg = ("Infobip transport could not find original request"
                            " when attempting to reply.")
                log.error(InfobipError(err_msg))
                return self.publish_nack(user_message_id=message['message_id'],
                    reason=err_msg)
            else:
                return self.publish_ack(message['message_id'],
                                 sent_message_id=response_id)
        else:
            err_msg = ("Infobip transport cannot process outbound message that"
                        " is not a reply: %s" % (message['message_id'],))
            log.error(InfobipError(err_msg))
            return self.publish_nack(user_message_id=message['message_id'],
                reason=err_msg)
