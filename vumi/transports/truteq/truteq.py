# -*- test-case-name: vumi.transports.truteq.tests.test_truteq -*-
# -*- coding: utf-8 -*-

"""TruTeq USSD transport."""

from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.python import log
from twisted.internet import reactor
from ssmi import client

from vumi.utils import normalize_msisdn
from vumi.message import TransportUserMessage
from vumi.transports.base import Transport
from vumi.components.session import SessionManager


# # Turn on debug logging in the SSMI library.
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
    :type link_check_period: int
    :param link_check_period:
        Number of seconds between link checks sent to the server.
        Default is 60.
    :ussd_session_lifetime: int
    :param ussd_session_lifetime:
        Maximum number of seconds to retain USSD session information.
        Default is 300.
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

    # default maximum lifetime of USSD sessions (in seconds)
    DEFAULT_USSD_SESSION_LIFETIME = 5 * 60

    # default period between link checks (in seconds)
    DEFAULT_LINK_CHECK_PERIOD = client.LINKCHECK_PERIOD

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
        self.link_check_period = self.config.get(
            'link_check_period', self.DEFAULT_LINK_CHECK_PERIOD)
        self.transport_type = self.config.get('transport_type', 'ussd')
        self.r_config = self.config.get('redis_manager', {})
        self.r_prefix = "%(transport_name)s:ussd_codes" % self.config

    @inlineCallbacks
    def setup_transport(self):
        self.ssmi_client = None
        ssmi_d = Deferred()
        # the strange wrapping of the funciton in a lambda is to get around
        # an odd type check in client.SSMIClient.__init__.
        self.factory = client.SSMIFactory(
            lambda ssmi_client: self._setup_ssmi_client(ssmi_client, ssmi_d))
        self.ssmi_connector = reactor.connectTCP(
            self.host, self.port, self.factory)

        self.session_manager = yield SessionManager.from_redis_config(
            self.r_config, self.r_prefix, self.ussd_session_lifetime)

        yield ssmi_d

    def _setup_ssmi_client(self, ssmi_client, ssmi_d):
        # TODO: Unpause the message consumer when the SSMI client connects.
        #       This requires the SSMI client to call this setup method on
        #       connectionMade (instead of on buildProtocol as it does now)
        #       and to have another callback for when a connection is lost.
        ssmi_client.app_setup(self.username, self.password,
                              ussd_callback=self.ussd_callback,
                              sms_callback=self.sms_callback,
                              errback=self.ssmi_errback,
                              link_check_period=self.link_check_period)
        self.ssmi_client = ssmi_client
        if not ssmi_d.called:
            # the setup gets called again if
            # the ssmi_client reconnects
            ssmi_d.callback(None)

    @inlineCallbacks
    def teardown_transport(self):
        self.factory.stopTrying()
        yield self.ssmi_connector.disconnect()
        yield self.session_manager.stop()

    @inlineCallbacks
    def ussd_callback(self, msisdn, ussd_type, phase, message, genfields=None):
        log.msg("Received USSD, from: %s, message: %s", msisdn, message)
        session_event = self.SSMI_TO_VUMI_EVENT[ussd_type]
        msisdn = normalize_msisdn(msisdn)
        message = message.decode(self.SSMI_ENCODING)
        genfields = genfields or {}

        if session_event == TransportUserMessage.SESSION_NEW:
            # If it's a new session then store the message as the USSD code
            if not message.endswith('#'):
                message = '%s#' % (message,)

            session = yield self.session_manager.create_session(
                msisdn, ussd_code=message)
            text = None
        else:
            session = yield self.session_manager.load_session(msisdn)
            text = message

        if session_event == TransportUserMessage.SESSION_CLOSE:
            yield self.session_manager.clear_session(msisdn)

        self.publish_message(
            from_addr=msisdn,
            to_addr=session['ussd_code'],
            session_event=session_event,
            content=text,
            transport_name=self.transport_name,
            transport_type=self.transport_type,
            transport_metadata={},
            helper_metadata={
                'truteq': {
                    'genfields': genfields,
                }
            })

    def sms_callback(self, *args, **kwargs):
        log.err("Got SMS from SSMI but SMSes not supported: %r, %r", args, kwargs)

    def ssmi_errback(self, *args, **kwargs):
        log.err("Got error from SSMI: %r, %r", args, kwargs)

    def handle_outbound_message(self, message):
        log.msg("Outbound USSD message: %s", message)
        text = message['content']
        if text is None:
            text = ''

        # Truteq uses \r as a message delimiter in the protocol.
        # Make sure we're only sending \n for new lines.
        text = text.replace('\r\n', '\n')
        text = text.replace('\r', '\n')

        ssmi_session_type = self.VUMI_TO_SSMI_EVENT[message['session_event']]
        # We need to send unicode data to ssmi_client, but bytes for msisdn.
        msisdn = message['to_addr'].strip('+').encode(self.SSMI_ENCODING)
        self.ssmi_client.send_ussd(msisdn, text, ssmi_session_type)
