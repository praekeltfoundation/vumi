# -*- coding: utf-8 -*-
# -*- test-case-name: vumi.transports.truteq.tests.test_truteq -*-

"""TruTeq USSD transport."""

from twisted.python import log
from twisted.internet import reactor

from ssmi import client

from vumi.utils import normalize_msisdn
from vumi.message import TransportUserMessage
from vumi.transports.base import Transport


class TruteqTransport(Transport):
    """
    The USSDTransport for TruTeq
    """

    SSMI_TO_VUMI_EVENT = {
        client.SSMI_USSD_TYPE_NEW: TransportUserMessage.SESSION_NEW,
        client.SSMI_USSD_TYPE_EXISTING: TransportUserMessage.SESSION_RESUME,
        client.SSMI_USSD_TYPE_END: TransportUserMessage.SESSION_CLOSE,
        client.SSMI_USSD_TYPE_TIMEOUT: TransportUserMessage.SESSION_CLOSE,
    }

    VUMI_TO_SSMI_EVENT = {
        TransportUserMessage.SESSION_NONE: client.SSMI_USSD_TYPE_EXISTING,
        TransportUserMessage.SESSION_NEW: client.SSMI_USSD_TYPE_NEW,
        TransportUserMessage.SESSION_RESUME: client.SSMI_USSD_TYPE_EXISTING,
        TransportUserMessage.SESSION_CLOSE: client.SSMI_USSD_TYPE_END,
    }

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        self.username = self.config['username']
        self.password = self.config['password']
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.transport_type = self.config.get('transport_type', 'ussd')

    def setup_transport(self):
        # FIXME:    this needs to be done more intelligently, it stores which
        #           MSISDN dialed into which ussd code, problem is that it is
        #           memory and will be lost during restarts.
        self.storage = {}
        self.ssmi_client = None
        # the strange wrapping of the funciton in a lambda is to get around
        # an odd type check in client.SSMIClient.__init__.
        factory = client.SSMIFactory(
            lambda ssmi_client: self._setup_ssmi_client(ssmi_client))
        self.ssmi_connector = reactor.connectTCP(self.host, self.port, factory)

    def _setup_ssmi_client(self, ssmi_client):
        ssmi_client.app_setup(self.username, self.password,
                              ussd_callback=self.ussd_callback,
                              sms_callback=self.sms_callback,
                              errback=self.ssmi_errback)
        self.ssmi_client = ssmi_client

    def teardown_transport(self):
        self.ssmi_connector.disconnect()

    def ussd_callback(self, msisdn, ussd_type, phase, message):
        log.msg("Received USSD, from: %s, message: %s" % (msisdn, message))
        session_event = self.SSMI_TO_VUMI_EVENT[ussd_type]

        # FIXME: See the note about self.storage
        # If it's a new session then store the message as the USSD code
        # use that as the routing key for publishing.
        if session_event == TransportUserMessage.SESSION_NEW:
            # cache
            ussd_code = self.storage[msisdn] = message
            text = None

        # If its the end of a session or a session has timed-out then we
        # should remove the USSD code from the storage
        elif session_event == TransportUserMessage.SESSION_CLOSE:
            # clear cache
            ussd_code = self.storage.get(msisdn)
            if msisdn in self.storage:
                del self.storage[msisdn]
            text = message

        # if it's an existing session then look up the USSD code from
        # the storage and use that as the routing key
        elif session_event == TransportUserMessage.SESSION_RESUME:
            # read cache
            ussd_code = self.storage.get(msisdn)
            text = message

        self.publish_message(
            from_addr=normalize_msisdn(msisdn),
            to_addr=normalize_msisdn(ussd_code),
            session_event=session_event,
            # XXX: text should be decoded before being published
            #      but I don't know what encoding is used.
            content=text,
            transport_name=self.transport_name,
            transport_type=self.transport_type,
            transport_metadata={},
            )

    def sms_callback(self, *args, **kwargs):
        log.err("Got SMS from SSMI but SMSes not supported: %r, %r"
                % (args, kwargs))

    def ssmi_errback(self, *args, **kwargs):
        log.err("Got error from SSMI: %r, %r" % (args, kwargs))

    def handle_outbound_message(self, message):
        text = message['content']
        if text is None:
            text = ''
        ssmi_session_type = self.VUMI_TO_SSMI_EVENT[message['session_event']]
        # XXX: text should be encoded before being sent out via SSMI
        #      but I don't know what the encoding should be.
        self.ssmi_client.send_ussd(message['to_addr'], text, ssmi_session_type)
