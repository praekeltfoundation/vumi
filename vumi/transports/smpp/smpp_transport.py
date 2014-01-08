# -*- test-case-name: vumi.transports.smpp.tests.test_smpp_transport -*-

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.base import Transport

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
        smpp_config = EsmeConfig(config.smpp_config, static=True)
        log.msg('Starting SMPP Transport for: %s' % (config.twisted_endpoint,))

        default_prefix = '%s@%s' % (smpp_config.system_id,
                                    config.transport_name)
        redis_prefix = config.split_bind_prefix or default_prefix
        self.redis = (yield TxRedisManager.from_config(
            config.redis_manager)).sub_manager(redis_prefix)

        self.dr_processor = config.delivery_report_processor(
            self, None, config.delivery_report_processor_config)
        self.sm_processor = config.short_message_processor(
            self, None, config.short_message_processor_config)
        self.sequence_generator = RedisSequence(self.redis)
        self.factory = self.factory_class(self)
        self.service = self.start_service(self.factory)

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
