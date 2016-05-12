# -*- test-case-name: vumi.transports.smpp.tests.test_smpp_transport -*-

import json
import warnings
from uuid import uuid4

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, succeed

from smpp.pdu import decode_pdu
from smpp.pdu_builder import PDU
from vumi.message import TransportUserMessage
from vumi.persist.txredis_manager import TxRedisManager
from vumi.transports.base import Transport
from vumi.transports.smpp.config import SmppTransportConfig
from vumi.transports.smpp.deprecated.transport import (
    SmppTransportConfig as OldSmppTransportConfig)
from vumi.transports.smpp.deprecated.utils import convert_to_new_config
from vumi.transports.smpp.smpp_service import SmppService
from vumi.transports.failures import FailureMessage


def sequence_number_key(seq_no):
    return 'sequence_number:%s' % (seq_no,)


def multipart_info_key(seq_no):
    return 'multipart_info:%s' % (seq_no,)


def message_key(message_id):
    return 'message:%s' % (message_id,)


def pdu_key(seq_no):
    return 'pdu:%s' % (seq_no,)


def remote_message_key(message_id):
    return 'remote_message:%s' % (message_id,)


class CachedPDU(object):
    """
    A cached PDU with its associated vumi message_id.
    """

    def __init__(self, vumi_message_id, pdu):
        self.vumi_message_id = vumi_message_id
        self.pdu = pdu
        self.seq_no = pdu.obj['header']['sequence_number']

    @classmethod
    def from_json(cls, pdu_json):
        if pdu_json is None:
            return None
        pdu_data = json.loads(pdu_json)
        pdu = PDU(None, None, None)
        pdu.obj = decode_pdu(pdu_data['pdu'])
        return cls(pdu_data['vumi_message_id'], pdu)

    def to_json(self):
        return json.dumps({
            'vumi_message_id': self.vumi_message_id,
            # We store the PDU in wire format to avoid json encoding troubles.
            'pdu': self.pdu.get_hex(),
        })


class SmppMessageDataStash(object):
    """
    Stash message data in Redis.
    """

    def __init__(self, redis, config):
        self.redis = redis
        self.config = config

    def init_multipart_info(self, message_id, part_count):
        key = multipart_info_key(message_id)
        expiry = self.config.submit_sm_expiry
        d = self.redis.hmset(key, {
            'parts': part_count,
        })
        d.addCallback(lambda _: self.redis.expire(key, expiry))
        return d

    def get_multipart_info(self, message_id):
        key = multipart_info_key(message_id)
        return self.redis.hgetall(key)

    def _update_multipart_info_success_cb(self, mp_info, key, remote_id):
        if not mp_info:
            # No multipart data, so do nothing.
            return
        part_key = 'part:%s' % (remote_id,)
        mp_info[part_key] = 'ack'
        d = self.redis.hset(key, part_key, 'ack')
        d.addCallback(lambda _: mp_info)
        return d

    def update_multipart_info_success(self, message_id, remote_id):
        key = multipart_info_key(message_id)
        d = self.get_multipart_info(message_id)
        d.addCallback(self._update_multipart_info_success_cb, key, remote_id)
        return d

    def _update_multipart_info_failure_cb(self, mp_info, key, remote_id):
        if not mp_info:
            # No multipart data, so do nothing.
            return
        part_key = 'part:%s' % (remote_id,)
        mp_info[part_key] = 'fail'
        d = self.redis.hset(key, part_key, 'fail')
        d.addCallback(lambda _: self.redis.hset(key, 'event_result', 'fail'))
        d.addCallback(lambda _: mp_info)
        return d

    def update_multipart_info_failure(self, message_id, remote_id):
        key = multipart_info_key(message_id)
        d = self.get_multipart_info(message_id)
        d.addCallback(self._update_multipart_info_failure_cb, key, remote_id)
        return d

    def _determine_multipart_event_cb(self, mp_info, message_id, event_type,
                                      remote_id):
        if not mp_info:
            # We don't seem to have a multipart message, so just return the
            # single-message data.
            return (True, event_type, remote_id)

        part_status_dict = dict(
            (k[5:], v) for k, v in mp_info.items() if k.startswith('part:'))
        remote_id = ','.join(sorted(part_status_dict.keys()))
        event_result = mp_info.get('event_result', None)

        if event_result is not None:
            # We already have a result, even if we don't have all the parts.
            event_type = event_result
        elif len(part_status_dict) >= int(mp_info['parts']):
            # We have all the parts, so we can determine the event type.
            if all(pv == 'ack' for pv in part_status_dict.values()):
                # All parts happy.
                event_type = 'ack'
            else:
                # At least one part failed.
                event_type = 'fail'
        else:
            # We don't have all the parts yet.
            return (False, None, None)

        # There's a race condition when we process multiple submit_sm_resps for
        # parts of the same messages concurrently. We only want to send one
        # event, so we do an atomic increment and ignore the event if we're
        # not the first to succeed.
        d = self.redis.hincrby(
            multipart_info_key(message_id), 'event_counter', 1)

        def confirm_multipart_event_cb(counter_value):
            if int(counter_value) == 1:
                return (True, event_type, remote_id)
            else:
                return (False, None, None)
        d.addCallback(confirm_multipart_event_cb)
        return d

    def get_multipart_event_info(self, message_id, event_type, remote_id):
        d = self.get_multipart_info(message_id)
        d.addCallback(
            self._determine_multipart_event_cb, message_id, event_type,
            remote_id)
        return d

    def expire_multipart_info(self, message_id):
        """
        Set the TTL on multipart info hash to something small. We don't delete
        this in case there's still an in-flight operation that will recreate it
        without a TTL.
        """
        expiry = self.config.completed_multipart_info_expiry
        return self.redis.expire(multipart_info_key(message_id), expiry)

    def set_sequence_number_message_id(self, sequence_number, message_id):
        key = sequence_number_key(sequence_number)
        expiry = self.config.submit_sm_expiry
        return self.redis.setex(key, expiry, message_id)

    def get_sequence_number_message_id(self, sequence_number):
        return self.redis.get(sequence_number_key(sequence_number))

    def delete_sequence_number_message_id(self, sequence_number):
        return self.redis.delete(sequence_number_key(sequence_number))

    def cache_message(self, message):
        key = message_key(message['message_id'])
        expiry = self.config.submit_sm_expiry
        return self.redis.setex(key, expiry, message.to_json())

    def get_cached_message(self, message_id):
        d = self.redis.get(message_key(message_id))
        d.addCallback(lambda json_data: (
            TransportUserMessage.from_json(json_data)
            if json_data else None))
        return d

    def delete_cached_message(self, message_id):
        return self.redis.delete(message_key(message_id))

    def cache_pdu(self, vumi_message_id, pdu):
        cached_pdu = CachedPDU(vumi_message_id, pdu)
        key = pdu_key(cached_pdu.seq_no)
        expiry = self.config.submit_sm_expiry
        return self.redis.setex(key, expiry, cached_pdu.to_json())

    def get_cached_pdu(self, seq_no):
        d = self.redis.get(pdu_key(seq_no))
        return d.addCallback(CachedPDU.from_json)

    def delete_cached_pdu(self, seq_no):
        return self.redis.delete(pdu_key(seq_no))

    def set_remote_message_id(self, message_id, smpp_message_id):
        if message_id is None:
            # If we store None, we end up with the string "None" in Redis. This
            # confuses later lookups (which treat any non-None value as a valid
            # identifier) and results in broken delivery reports.
            return succeed(None)

        key = remote_message_key(smpp_message_id)
        expire = self.config.third_party_id_expiry
        d = self.redis.setex(key, expire, message_id)
        d.addCallback(lambda _: message_id)
        return d

    def get_internal_message_id(self, smpp_message_id):
        return self.redis.get(remote_message_key(smpp_message_id))

    def delete_remote_message_id(self, smpp_message_id):
        key = remote_message_key(smpp_message_id)
        return self.redis.delete(key)

    def expire_remote_message_id(self, smpp_message_id):
        key = remote_message_key(smpp_message_id)
        expire = self.config.final_dr_third_party_id_expiry
        return self.redis.expire(key, expire)


class SmppTransceiverTransport(Transport):

    CONFIG_CLASS = SmppTransportConfig

    bind_type = 'TRX'
    clock = reactor
    start_message_consumer = False

    @property
    def throttled(self):
        return self.service.throttled

    @inlineCallbacks
    def setup_transport(self):
        yield self.publish_status_starting()

        config = self.get_static_config()
        self.log.msg(
            'Starting SMPP Transport for: %s' % (config.twisted_endpoint,))

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
        self.disable_ack = config.disable_ack
        self.disable_delivery_report = config.disable_delivery_report
        self.message_stash = SmppMessageDataStash(self.redis, config)
        self.service = self.start_service()

    def start_service(self):
        config = self.get_static_config()
        service = SmppService(config.twisted_endpoint, self.bind_type, self)
        service.clock = self.clock
        service.startService()
        return service

    @inlineCallbacks
    def teardown_transport(self):
        if self.service:
            yield self.service.stopService()
        yield self.redis._close()

    def _check_address_valid(self, message, field):
        try:
            message[field].encode('ascii')
        except UnicodeError:
            return False
        return True

    def _reject_for_invalid_address(self, message, field):
        return self.publish_nack(
            message['message_id'], u'Invalid %s: %s' % (field, message[field]))

    @inlineCallbacks
    def on_smpp_binding(self):
        yield self.publish_status_binding()

    @inlineCallbacks
    def on_smpp_unbinding(self):
        yield self.publish_status_unbinding()

    @inlineCallbacks
    def on_smpp_bind(self):
        yield self.publish_status_bound()

        if self.throttled:
            yield self.publish_throttled()

    @inlineCallbacks
    def on_throttled(self):
        yield self.publish_throttled()

    @inlineCallbacks
    def on_throttled_resume(self):
        yield self.publish_throttled()

    @inlineCallbacks
    def on_throttled_end(self):
        yield self.publish_throttled_end()

    @inlineCallbacks
    def on_smpp_bind_timeout(self):
        yield self.publish_status_bind_timeout()

    @inlineCallbacks
    def on_connection_lost(self, reason):
        yield self.publish_status_connection_lost(reason)

    def publish_status_starting(self):
        return self.publish_status(
            status='down',
            component='smpp',
            type='starting',
            message='Starting')

    def publish_status_binding(self):
        return self.publish_status(
            status='down',
            component='smpp',
            type='binding',
            message='Binding')

    def publish_status_unbinding(self):
        return self.publish_status(
            status='down',
            component='smpp',
            type='unbinding',
            message='Unbinding')

    def publish_status_bound(self):
        return self.publish_status(
            status='ok',
            component='smpp',
            type='bound',
            message='Bound')

    def publish_throttled(self):
        return self.publish_status(
            status='degraded',
            component='smpp',
            type='throttled',
            message='Throttled')

    def publish_throttled_end(self):
        return self.publish_status(
            status='ok',
            component='smpp',
            type='throttled_end',
            message='No longer throttled')

    def publish_status_bind_timeout(self):
        return self.publish_status(
            status='down',
            component='smpp',
            type='bind_timeout',
            message='Timed out awaiting bind')

    def publish_status_connection_lost(self, reason):
        return self.publish_status(
            status='down',
            component='smpp',
            type='connection_lost',
            message=str(reason.value))

    @inlineCallbacks
    def handle_outbound_message(self, message):
        if not self._check_address_valid(message, 'to_addr'):
            yield self._reject_for_invalid_address(message, 'to_addr')
            return
        if not self._check_address_valid(message, 'from_addr'):
            yield self._reject_for_invalid_address(message, 'from_addr')
            return
        yield self.message_stash.cache_message(message)
        yield self.submit_sm_processor.handle_outbound_message(
            message, self.service)

    @inlineCallbacks
    def process_submit_sm_event(self, message_id, event_type, remote_id,
                                command_status):
        if event_type == 'ack':
            yield self.message_stash.delete_cached_message(message_id)
            yield self.message_stash.expire_multipart_info(message_id)
            if not self.disable_ack:
                yield self.publish_ack(message_id, remote_id)
        else:
            if event_type != 'fail':
                self.log.warning(
                    "Unexpected multipart event type %r, assuming 'fail'" % (
                        event_type,))
            err_msg = yield self.message_stash.get_cached_message(message_id)
            command_status = command_status or 'Unspecified'
            if err_msg is None:
                self.log.warning(
                    "Could not retrieve failed message: %s" % (message_id,))
            else:
                yield self.message_stash.delete_cached_message(message_id)
                yield self.message_stash.expire_multipart_info(message_id)
                yield self.publish_nack(message_id, command_status)
                yield self.failure_publisher.publish_message(
                    FailureMessage(message=err_msg.payload,
                                   failure_code=None,
                                   reason=command_status))

    @inlineCallbacks
    def handle_submit_sm_success(self, message_id, smpp_message_id,
                                 command_status):
        yield self.message_stash.update_multipart_info_success(
            message_id, smpp_message_id)

        event_info = yield self.message_stash.get_multipart_event_info(
            message_id, 'ack', smpp_message_id)
        event_required, event_type, remote_id = event_info
        if event_required:
            yield self.process_submit_sm_event(
                message_id, event_type, remote_id, command_status)

    @inlineCallbacks
    def handle_submit_sm_failure(self, message_id, smpp_message_id,
                                 command_status):
        yield self.message_stash.update_multipart_info_failure(
            message_id, smpp_message_id)

        event_info = yield self.message_stash.get_multipart_event_info(
            message_id, 'fail', smpp_message_id)
        event_required, event_type, remote_id = event_info
        if event_required:
            yield self.process_submit_sm_event(
                message_id, event_type, remote_id, command_status)

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
        return self.publish_message(**message).addErrback(self.log.err)

    @inlineCallbacks
    def handle_delivery_report(
            self, receipted_message_id, delivery_status,
            smpp_delivery_status):
        message_id = yield self.message_stash.get_internal_message_id(
            receipted_message_id)
        if message_id is None:
            self.log.warning(
                "Failed to retrieve message id for delivery report."
                " Delivery report from %s discarded."
                % self.transport_name)
            return

        if self.disable_delivery_report:
            dr = None
        else:
            dr = yield self.publish_delivery_report(
                user_message_id=message_id,
                delivery_status=delivery_status,
                transport_metadata={
                    'smpp_delivery_status': smpp_delivery_status,
                })

        if delivery_status in ('delivered', 'failed'):
            yield self.message_stash.expire_remote_message_id(
                receipted_message_id)

        returnValue(dr)


class SmppReceiverTransport(SmppTransceiverTransport):
    bind_type = 'RX'


class SmppTransmitterTransport(SmppTransceiverTransport):
    bind_type = 'TX'


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
        original = cfg._config_data.copy()
        config = convert_to_new_config(
            original,
            'vumi.transports.smpp.processors.DeliveryReportProcessor',
            'vumi.transports.smpp.processors.SubmitShortMessageProcessor',
            'vumi.transports.smpp.processors.DeliverShortMessageProcessor'
        )

        self._converted_static_config = self.NEW_CONFIG_CLASS(
            config, static=True)
        return self._converted_static_config
