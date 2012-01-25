# -*- test-case-name: vumi.transports.truteq.tests.test_truteq -*-
# -*- coding: utf-8 -*-

"""TruTeq USSD transport."""

from twisted.internet.defer import Deferred
from twisted.python import log
from twisted.internet import reactor

from ssmi import client
import redis

from vumi.utils import normalize_msisdn
from vumi.message import TransportUserMessage
from vumi.transports.base import Transport


# client.set_debug(True)


class TruteqTransport(Transport):
    """
    A transport for TruTeq.

    Currently only USSD messages are supported.

    TruTeq transport options:

    :type username: str
    :param username:
        Username of the TruTeq account to connect to.
    :type password: str
    :param password:
        Password for the TruTeq account.
    :type host: str
    :param host:
        Hostname of the TruTeq SSMI server to connect to.
    :type port: int
    :param port:
        Port of the TruTeq SSMI server.
    :ussd_session_lifetime: int
    :param ussd_session_lifetime:
        Maximum number of seconds to retain USSD session information.
        Default is 300.
    """

    SUPPRESS_FAILURE_EXCEPTIONS = False

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

    # default maximum lifetime of USSD sessions (in seconds)
    DEFAULT_USSD_SESSION_LIFETIME = 5 * 60

    # TODO: Check that UTF-8 is in fact what TruTeq use to encode their
    #       messages
    SSMI_ENCODING = "UTF-8"

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        self.username = self.config['username']
        self.password = self.config['password']
        self.host = self.config['host']
        self.port = int(self.config['port'])
        self.ussd_session_lifetime = self.config.get(
                'ussd_session_lifetime', self.DEFAULT_USSD_SESSION_LIFETIME)
        self.transport_type = self.config.get('transport_type', 'ussd')
        self.r_config = self.config.get('redis', {})
        self.r_prefix = "%(transport_name)s:ussd_codes" % self.config

    def setup_transport(self):
        self.r_server = redis.Redis(**self.r_config)
        self.ssmi_client = None
        self._setup_d = Deferred()
        # the strange wrapping of the funciton in a lambda is to get around
        # an odd type check in client.SSMIClient.__init__.
        factory = client.SSMIFactory(
            lambda ssmi_client: self._setup_ssmi_client(ssmi_client))
        self.ssmi_connector = reactor.connectTCP(self.host, self.port, factory)
        return self._setup_d

    def _setup_ssmi_client(self, ssmi_client):
        ssmi_client.app_setup(self.username, self.password,
                              ussd_callback=self.ussd_callback,
                              sms_callback=self.sms_callback,
                              errback=self.ssmi_errback)
        self.ssmi_client = ssmi_client
        self._setup_d.callback(None)

    def teardown_transport(self):
        self.ssmi_connector.disconnect()

    def msisdn_key(self, msisdn):
        return "%s:%s" % (self.r_prefix, msisdn)

    def get_ussd_code(self, msisdn, delete=False):
        rkey = self.msisdn_key(msisdn)
        ussd_code = self.r_server.get(rkey)
        if ussd_code is not None and delete:
            self.r_server.delete(rkey)
        return ussd_code

    def set_ussd_code(self, msisdn, ussd_code):
        rkey = self.msisdn_key(msisdn)
        ussd_code = self.r_server.set(rkey, ussd_code)
        self.r_server.expire(rkey, self.ussd_session_lifetime)

    def ussd_callback(self, msisdn, ussd_type, phase, message):
        log.msg("Received USSD, from: %s, message: %s" % (msisdn, message))
        session_event = self.SSMI_TO_VUMI_EVENT[ussd_type]
        msisdn = normalize_msisdn(msisdn)
        message = message.decode(self.SSMI_ENCODING)

        if session_event == TransportUserMessage.SESSION_NEW:
            # If it's a new session then store the message as the USSD code
            ussd_code = message
            self.set_ussd_code(msisdn, ussd_code)
            text = None
        elif session_event == TransportUserMessage.SESSION_CLOSE:
            ussd_code = self.get_ussd_code(msisdn, delete=True)
            text = message
        elif session_event == TransportUserMessage.SESSION_RESUME:
            ussd_code = self.get_ussd_code(msisdn)
            text = message

        self.publish_message(
            from_addr=msisdn,
            to_addr=ussd_code,
            session_event=session_event,
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
        log.msg("Outbound USSD message: %s" % (message,))
        text = message['content']
        if text is None:
            text = ''
        ssmi_session_type = self.VUMI_TO_SSMI_EVENT[message['session_event']]
        data = text.encode(self.SSMI_ENCODING)
        msisdn = message['to_addr'].strip('+').encode(self.SSMI_ENCODING)
        self.ssmi_client.send_ussd(msisdn, data, ssmi_session_type)
