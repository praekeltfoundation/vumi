# -*- test-case-name: vumi.transports.truteq.tests.test_truteq -*-
# -*- coding: utf-8 -*-

"""TruTeq USSD transport."""

from twisted.internet.defer import Deferred, inlineCallbacks, maybeDeferred
from twisted.internet.protocol import Factory


from txssmi.protocol import SSMIProtocol
from txssmi import constants

from vumi import log
from vumi.components.session import SessionManager
from vumi.config import (
    ConfigText, ConfigInt, ConfigClientEndpoint, ConfigBool, ConfigDict)
from vumi.message import TransportUserMessage
from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.base import Transport
from vumi.utils import normalize_msisdn


class TruteqTransportConfig(Transport.CONFIG_CLASS):
    username = ConfigText(
        'Username of the TruTeq account to connect to.', static=True)
    password = ConfigText(
        'Password for the TruTeq account.', static=True)
    twisted_endpoint = ConfigClientEndpoint(
        'The endpoint to connect to.',
        default='tcp:host=sms.truteq.com:port=50008', static=True)
    link_check_period = ConfigInt(
        'Number of seconds between link checks sent to the server.',
        default=60, static=True)
    ussd_session_lifetime = ConfigInt(
        'Maximum number of seconds to retain USSD session information.',
        default=300, static=True)
    debug = ConfigBool(
        'Print verbose log output.', default=False, static=True)
    redis_manager = ConfigDict(
        'How to connect to Redis.', default={}, static=True)


class TruteqTransportProtocol(SSMIProtocol):

    def __init__(self, vumi_transport):
        SSMIProtocol.__init__(self)
        config = vumi_transport.get_static_config()
        self.noisy = config.debug
        self.vumi_transport = vumi_transport

    def connectionMade(self):
        config = self.vumi_transport.get_static_config()
        d = self.authenticate(config.username, config.password)
        d.addCallback(
            lambda success: (
                self.link_check.start(config.link_check_period)
                if success else self.loseConnection()))
        return d

    def handle_MO(self, mo):
        return self.vumi_transport.handle_unhandled_message(mo)

    def handle_BINARY_MO(self, mo):
        return self.vumi_transport.handle_unhandled_message(mo)

    def handle_PREMIUM_MO(self, mo):
        return self.vumi_transport.handle_unhandled_message(mo)

    def handle_PREMIUM_BINARY_MO(self, mo):
        return self.vumi_transport.handle_unhandled_message(mo)

    def handle_USSD_MESSAGE(self, um):
        return self.vumi_transport.handle_raw_inbound_message(um)

    def handle_EXTENDED_USSD_MESSAGE(self, um):
        return self.vumi_transport.handle_raw_inbound_message(um)

    def handle_LOGOUT(self, msg):
        return self.vumi_transport.handle_remote_logout(msg)


class TruteqService(ReconnectingClientService):

    def get_protocol(self):
        return self._protocol

    def stopService(self):
        protocol = self.get_protocol()
        if protocol:
            if protocol.link_check.running:
                protocol.link_check.stop()
        return ReconnectingClientService.stopService(self)


class TruteqTransport(Transport):
    """
    A transport for TruTeq.

    Currently only USSD messages are supported.

    """

    CONFIG_CLASS = TruteqTransportConfig
    service_class = TruteqService
    protocol_class = TruteqTransportProtocol
    encoding = 'iso-8859-1'

    SSMI_TO_VUMI_EVENT = {
        constants.USSD_NEW: TransportUserMessage.SESSION_NEW,
        constants.USSD_RESPONSE: TransportUserMessage.SESSION_RESUME,
        constants.USSD_END: TransportUserMessage.SESSION_CLOSE,
        constants.USSD_TIMEOUT: TransportUserMessage.SESSION_CLOSE,
    }

    VUMI_TO_SSMI_EVENT = {
        TransportUserMessage.SESSION_NONE: constants.USSD_RESPONSE,
        TransportUserMessage.SESSION_NEW: constants.USSD_NEW,
        TransportUserMessage.SESSION_RESUME: constants.USSD_RESPONSE,
        TransportUserMessage.SESSION_CLOSE: constants.USSD_END,
    }

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        factory = Factory.forProtocol(self.protocol_class(self))

        prefix = "%s:ussd_codes" % (config.transport_name,)
        self.session_manager = yield SessionManager.from_redis_config(
            config.redis_manager, prefix, config.ussd_session_lifetime)
        self.client_service = self.get_service(
            config.twisted_endpoint, factory)

    def get_service(self, endpoint, factory):
        client_service = self.service_class(endpoint, factory)
        client_service.startService()
        return client_service

    def teardown_transport(self):
        d = maybeDeferred(self.client_service.stopService)
        d.addCallback(lambda _: self.session_manager.stop())
        return d

    @inlineCallbacks
    def handle_raw_inbound_message(self, ussd_message):
        if ussd_message.command_name == 'EXTENDED_USSD_MESSAGE':
            genfields = {
                'IMSI': '',
                'Subscriber Type': '',
                'OperatorID': '',
                'SessionID': '',
                'ValiPort': '',
            }
            genfield_values = ussd_message.genfields.split(':')
            genfields.update(
                dict(zip(genfields.keys(), genfield_values)))
        else:
            genfields = {}

        session_event = self.SSMI_TO_VUMI_EVENT[ussd_message.type]
        msisdn = normalize_msisdn(ussd_message.msisdn)
        message = ussd_message.message.decode(self.encoding)

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

        yield self.publish_message(
            from_addr=msisdn,
            to_addr=session['ussd_code'],
            session_event=session_event,
            content=text,
            transport_type='ussd',
            transport_metadata={},
            helper_metadata={
                'truteq': {
                    'genfields': genfields,
                }
            })

    def handle_outbound_message(self, message):
        protocol = self.client_service.get_protocol()
        text = message.get('content') or ''

        # Truteq uses \r as a message delimiter in the protocol.
        # Make sure we're only sending \n for new lines.
        text = '\n'.join(text.splitlines()).encode(self.encoding)

        ssmi_session_type = self.VUMI_TO_SSMI_EVENT[message['session_event']]
        # We need to send unicode data to ssmi_client, but bytes for msisdn.
        msisdn = message['to_addr'].strip('+').encode(self.encoding)
        return protocol.send_ussd_message(msisdn, text, ssmi_session_type)

    def handle_remote_logout(self, msg):
        log.warning('Received remote logout command, disconnecting: %r' % (
            msg,))
        return self.teardown_transport()

    def handle_unhandled_message(self, mo):
        log.warning('Received unsupported message, dropping: %r.' % (
            mo,))
