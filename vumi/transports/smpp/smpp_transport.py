# -*- test-case-name: vumi.transports.smpp.tests.test_smpp_transport -*-

import warnings
from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.defer import (
    inlineCallbacks, gatherResults, maybeDeferred, returnValue, Deferred,
    succeed)
from twisted.internet.task import LoopingCall

from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.base import Transport

from vumi.message import TransportUserMessage

from vumi.transports.smpp.config import SmppTransportConfig
from vumi.transports.smpp.deprecated.transport import (
    SmppTransportConfig as OldSmppTransportConfig)
from vumi.transports.smpp.deprecated.utils import convert_to_new_config
from vumi.transports.smpp.protocol import EsmeTransceiverFactory
from vumi.transports.smpp.sequence import RedisSequence
from vumi.transports.failures import FailureMessage

from vumi.persist.txredis_manager import TxRedisManager

from smpp.pdu_builder import BindTransceiver, BindReceiver, BindTransmitter

from vumi import log


def sequence_number_key(seq_no):
    return 'sequence_number:%s' % (seq_no,)


def message_key(message_id):
    return 'message:%s' % (message_id,)


def remote_message_key(message_id):
    return 'remote_message:%s' % (message_id,)


class SmppTransceiverProtocol(EsmeTransceiverFactory.protocol):
    bind_pdu = BindTransceiver

    def connectionMade(self):
        EsmeTransceiverFactory.protocol.connectionMade(self)
        config = self.vumi_transport.get_static_config()
        self.bind(config.system_id, config.password, config.system_type)

    def connectionLost(self, reason):
        d = maybeDeferred(self.vumi_transport.pause_connectors)
        d.addCallback(
            lambda _: EsmeTransceiverFactory.protocol.connectionLost(
                self, reason))
        return d

    def on_smpp_bind(self, sequence_number):
        d = maybeDeferred(EsmeTransceiverFactory.protocol.on_smpp_bind,
                          self, sequence_number)
        d.addCallback(lambda _: self.vumi_transport.unpause_connectors())
        return d

    def on_submit_sm_resp(self, sequence_number, smpp_message_id,
                          command_status):
        cb = {
            'ESME_ROK': self.vumi_transport.handle_submit_sm_success,
            'ESME_RTHROTTLED': self.vumi_transport.handle_submit_sm_throttled,
            'ESME_RMSGQFUL': self.vumi_transport.handle_submit_sm_throttled,
        }.get(command_status, self.vumi_transport.handle_submit_sm_failure)
        d = self.vumi_transport.get_sequence_number_message_id(sequence_number)
        d.addCallback(
            self.vumi_transport.set_remote_message_id, smpp_message_id)
        d.addCallback(
            lambda message_id: cb(message_id, smpp_message_id, command_status))
        return d


class SmppReceiverProtocol(SmppTransceiverProtocol):
    bind_pdu = BindReceiver


class SmppTransmitterProtocol(SmppTransceiverProtocol):
    bind_pdu = BindTransmitter


class SmppTransceiverClientFactory(EsmeTransceiverFactory):
    protocol = SmppTransceiverProtocol


class SmppService(ReconnectingClientService):

    def __init__(self, endpoint, factory):
        ReconnectingClientService.__init__(self, endpoint, factory)
        self.wait_on_protocol_deferreds = []

    def clientConnected(self, protocol):
        ReconnectingClientService.clientConnected(self, protocol)
        for deferred in self.wait_on_protocol_deferreds:
            deferred.callback(protocol)

    def get_protocol(self):
        if self._protocol is not None:
            return succeed(self._protocol)
        else:
            d = Deferred()
            self.wait_on_protocol_deferreds.append(d)
            return d

    def stopService(self):
        protocol = self.get_protocol()
        if protocol is not None:
            d = protocol.disconnect()
            d.addCallback(
                lambda _: ReconnectingClientService.stopService(self))
            return d
        return ReconnectingClientService.stopService(self)


class SmppTransceiverTransport(Transport):

    CONFIG_CLASS = SmppTransportConfig

    factory_class = SmppTransceiverClientFactory
    service_class = SmppService
    clock = reactor
    start_message_consumer = False

    @inlineCallbacks
    def setup_transport(self):
        config = self.get_static_config()
        log.msg('Starting SMPP Transport for: %s' % (config.twisted_endpoint,))

        default_prefix = '%s@%s' % (config.system_id,
                                    config.transport_name)
        redis_prefix = config.split_bind_prefix or default_prefix
        self.redis = (yield TxRedisManager.from_config(
            config.redis_manager)).sub_manager(redis_prefix)

        self.dr_processor = config.delivery_report_processor(
            self, config.delivery_report_processor_config)
        self.deliver_sm_processor = config.deliver_short_message_processor(
            self, config.deliver_short_message_processor_config)
        self.submit_sm_processor = config.submit_short_message_processor(
            self, config.submit_short_message_processor_config)

        self.sequence_generator = RedisSequence(self.redis)
        self.throttled = None
        self.factory = self.factory_class(self)

        self.service = self.start_service(self.factory)

        self.tps_counter = 0
        self.tps_limit = config.mt_tps
        if config.mt_tps > 0:
            self.mt_tps_lc = LoopingCall(self.reset_mt_tps)
            self.mt_tps_lc.clock = self.clock
            self.mt_tps_lc.start(1, now=True)
        else:
            self.mt_tps_lc = None

    def start_service(self, factory):
        config = self.get_static_config()
        service = self.service_class(config.twisted_endpoint, factory)
        service.startService()
        return service

    @inlineCallbacks
    def teardown_transport(self):
        if self.service:
            yield self.service.stopService()
        if self.mt_tps_lc and self.mt_tps_lc.running:
            self.mt_tps_lc.stop()
        yield self.redis._close()

    def reset_mt_tps(self):
        if self.throttled and self.need_mt_throttling():
            self.reset_mt_throttle_counter()
            self.stop_throttling()

    def reset_mt_throttle_counter(self):
        self.tps_counter = 0

    def incr_mt_throttle_counter(self):
        self.tps_counter += 1

    def need_mt_throttling(self):
        return self.tps_counter > self.tps_limit

    def bind_requires_throttling(self):
        config = self.get_static_config()
        return config.mt_tps > 0

    def check_mt_throttling(self):
        self.incr_mt_throttle_counter()
        if self.need_mt_throttling():
            self.start_throttling()

    @inlineCallbacks
    def handle_outbound_message(self, message):
        if self.bind_requires_throttling():
            self.check_mt_throttling()
        protocol = yield self.service.get_protocol()
        seq_nrs = yield self.submit_sm_processor.handle_outbound_message(
            message, protocol)
        yield gatherResults([
            self.set_sequence_number_message_id(sqn, message['message_id'])
            for sqn in seq_nrs])
        yield self.cache_message(message)

    def set_sequence_number_message_id(self, sequence_number, message_id):
        key = sequence_number_key(sequence_number)
        expiry = self.get_static_config().third_party_id_expiry
        d = self.redis.set(key, message_id)
        d.addCallback(lambda _: self.redis.expire(key, expiry))
        return d

    def get_sequence_number_message_id(self, sequence_number):
        return self.redis.get(sequence_number_key(sequence_number))

    def cache_message(self, message):
        key = message_key(message['message_id'])
        expire = self.get_static_config().submit_sm_expiry
        d = self.redis.set(key, message.to_json())
        d.addCallback(lambda _: self.redis.expire(key, expire))
        return d

    def get_cached_message(self, message_id):
        d = self.redis.get(message_key(message_id))
        d.addCallback(lambda json_data: (
            TransportUserMessage.from_json(json_data)
            if json_data else None))
        return d

    def delete_cached_message(self, message_id):
        return self.redis.delete(message_key(message_id))

    def set_remote_message_id(self, message_id, smpp_message_id):
        key = remote_message_key(smpp_message_id)
        config = self.get_static_config()
        d = self.redis.set(key, message_id)
        d.addCallback(
            lambda _: self.redis.expire(key, config.third_party_id_expiry))
        d.addCallback(lambda _: message_id)
        return d

    def get_internal_message_id(self, smpp_message_id):
        return self.redis.get(remote_message_key(smpp_message_id))

    def handle_submit_sm_success(self, message_id, smpp_message_id,
                                 command_status):
        if self.throttled:
            self.stop_throttling()
        d = self.publish_ack(message_id, smpp_message_id)
        d.addCallback(lambda _: self.delete_cached_message(message_id))
        return d

    @inlineCallbacks
    def handle_submit_sm_failure(self, message_id, smpp_message_id,
                                 command_status):
        error_message = yield self.get_cached_message(message_id)
        command_status = command_status or 'Unspecified'
        if error_message is None:
            log.err("Could not retrieve failed message:%s" % (
                message_id))
        else:
            yield self.delete_cached_message(message_id)
            yield self.publish_nack(message_id, command_status)
            yield self.failure_publisher.publish_message(
                FailureMessage(message=error_message.payload,
                               failure_code=None,
                               reason=command_status))

    @inlineCallbacks
    def handle_submit_sm_throttled(self, message_id, smpp_message_id,
                                   command_status):
        config = self.get_static_config()
        message = yield self.get_cached_message(message_id)
        self.start_throttling()
        if message is None:
            log.err("Could not retrieve throttled message:%s" % (
                message_id))
            self.clock.callLater(config.throttle_delay, self.stop_throttling)
        else:
            self.clock.callLater(config.throttle_delay,
                                 self.handle_outbound_message, message)

    def start_throttling(self):
        if self.throttled:
            return
        log.warning("Throttling outbound messages.")
        self.throttled = True
        self.pause_connectors()

    def stop_throttling(self):
        if not self.throttled:
            return
        log.warning("No longer throttling outbound messages.")
        self.throttled = False
        self.unpause_connectors()

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

    @inlineCallbacks
    def handle_delivery_report(self, receipted_message_id, delivery_status):
        message_id = yield self.get_internal_message_id(receipted_message_id)
        if message_id is None:
            log.warning("Failed to retrieve message id for delivery report."
                        " Delivery report from %s discarded."
                        % self.transport_name)
            return

        dr = yield self.publish_delivery_report(
            user_message_id=message_id,
            delivery_status=delivery_status)
        returnValue(dr)


class SmppReceiverClientFactory(EsmeTransceiverFactory):
    protocol = SmppReceiverProtocol


class SmppReceiverTransport(SmppTransceiverTransport):
    factory_class = SmppReceiverClientFactory


class SmppTransmitterClientFactory(EsmeTransceiverFactory):
    protocol = SmppTransmitterProtocol


class SmppTransmitterTransport(SmppTransceiverTransport):
    factory_class = SmppTransmitterClientFactory


class SmppTransceiverTransportWithOldConfig(SmppTransceiverTransport):

    CONFIG_CLASS = OldSmppTransportConfig
    NEW_CONFIG_CLASS = SmppTransportConfig

    def __init__(self, *args, **kwargs):
        super(SmppTransceiverTransportWithOldConfig, self).__init__(*args,
                                                                    **kwargs)
        warnings.warn(
            'This is a transport using a deprecated config file. '
            'Please use the new SmppTransceiverTransport, '
            'SmppTransmitterTransport or SmppReceiverTransport '
            'with the new processor aware SmppTransportConfig.',
            category=PendingDeprecationWarning)

    def get_static_config(self):
        # return if cached
        if hasattr(self, '_converted_static_config'):
            return self._converted_static_config

        cfg = super(
            SmppTransceiverTransportWithOldConfig, self).get_static_config()
        original = cfg._config_data.original.copy()
        config = convert_to_new_config(
            original,
            'vumi.transports.smpp.processors.DeliveryReportProcessor',
            'vumi.transports.smpp.processors.SubmitShortMessageProcessor',
            'vumi.transports.smpp.processors.DeliverShortMessageProcessor'
        )

        self._converted_static_config = self.NEW_CONFIG_CLASS(
            config, static=True)
        return self._converted_static_config
