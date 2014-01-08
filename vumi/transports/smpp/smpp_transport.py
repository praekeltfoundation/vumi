# -*- test-case-name: vumi.transports.smpp.tests.test_smpp_transport -*-

from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.base import Transport

from vumi.message import TransportUserMessage

from vumi.transports.smpp.config import SmppTransportConfig, EsmeConfig
from vumi.transports.smpp.clientserver.new_client import EsmeTransceiverFactory
from vumi.transports.smpp.clientserver.sequence import RedisSequence

from vumi.persist.txredis_manager import TxRedisManager

from vumi import log


class SmppTransceiverProtocol(EsmeTransceiverFactory.protocol):

    def onConnectionMade(self):
        return self.bind()


class SmppTransportClientFactory(EsmeTransceiverFactory):
    protocol = SmppTransceiverProtocol


class SmppTransport(Transport):

    CONFIG_CLASS = SmppTransportConfig

    factory_class = SmppTransportClientFactory
    clock = reactor

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        self.smpp_config = EsmeConfig(config.smpp_config, static=True)
        log.msg('Starting SMPP Transport for: %s' % (config.twisted_endpoint,))

        default_prefix = '%s@%s' % (self.smpp_config.system_id,
                                    config.transport_name)
        redis_prefix = config.split_bind_prefix or default_prefix
        self.redis = (yield TxRedisManager.from_config(
            config.redis_manager)).sub_manager(redis_prefix)

        self.dr_processor = config.delivery_report_processor(
            self, config.delivery_report_processor_config)
        self.sm_processor = config.short_message_processor(
            self, config.short_message_processor_config)
        self.sequence_generator = RedisSequence(self.redis)
        self.factory = self.factory_class(self)
        self.service = self.start_service(self.factory)
        self.protocol = self.service.protocol

    def start_service(self, factory):
        config = self.get_static_config()
        service = ReconnectingClientService(config.twisted_endpoint, factory)
        service.startService()
        return service

    @inlineCallbacks
    def teardown_transport(self):
        if self.service:
            yield self.service.stopService()
        yield self.redis._close()

    def handle_outbound_message(self, message):

        to_addr = message['to_addr']
        from_addr = message['from_addr']
        text = message['content']
        session_info = message['transport_metadata'].get('session_info')
        session_event = message['session_event']
        transport_type = message['transport_type']

        if session_event == TransportUserMessage.SESSION_CLOSE:
            continue_session = False
        else:
            continue_session = True

        return self.protocol.submitSM(
            # these end up in the PDU
            short_message=text.encode(self.smpp_config.submit_sm_encoding),
            data_coding=self.smpp_config.submit_sm_data_coding,
            destination_addr=to_addr.encode('ascii'),
            source_addr=from_addr.encode('ascii'),
            session_info=(session_info.encode('ascii')
                          if session_info is not None else None),
            # these don't end up in the PDU
            message_type=transport_type,
            continue_session=continue_session,
        )

    def handle_raw_inbound_message(self, **kwargs):
        # TODO: drop the kwargs, list the allowed key word arguments
        #       explicitly with sensible defaults.
        message_type = kwargs.get('message_type', 'sms')
        message = {
            'message_id': uuid4().hex,
            'to_addr': kwargs['destination_addr'],
            'from_addr': kwargs['source_addr'],
            'content': kwargs['short_message'],
            'transport_type': message_type,
            'transport_metadata': {},
        }

        if message_type == 'ussd':
            session_event = {
                'new': TransportUserMessage.SESSION_NEW,
                'continue': TransportUserMessage.SESSION_RESUME,
                'close': TransportUserMessage.SESSION_CLOSE,
                }[kwargs['session_event']]
            message['session_event'] = session_event
            session_info = kwargs.get('session_info')
            message['transport_metadata']['session_info'] = session_info

        # TODO: This logs messages that fail to serialize to JSON
        #       Usually this happens when an SMPP message has content
        #       we can't decode (e.g. data_coding == 4). We should
        #       remove the try-except once we handle such messages
        #       better.
        return self.publish_message(**message).addErrback(log.err)
