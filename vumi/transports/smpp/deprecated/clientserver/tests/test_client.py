from twisted.internet.task import Clock
from twisted.internet.defer import inlineCallbacks, returnValue
from smpp.pdu_builder import DeliverSM, BindTransceiverResp, Unbind
from smpp.pdu import unpack_pdu

from vumi.tests.utils import LogCatcher
from vumi.transports.smpp.deprecated.clientserver.client import (
    EsmeTransceiver, EsmeReceiver, EsmeTransmitter, EsmeCallbacks, ESME,
    unpacked_pdu_opts)
from vumi.transports.smpp.deprecated.transport import SmppTransportConfig
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class FakeTransport(object):
    def __init__(self, protocol):
        self.connected = True
        self.protocol = protocol

    def loseConnection(self):
        self.connected = False
        self.protocol.connectionLost()


class FakeEsmeMixin(object):
    def setup_fake(self):
        self.transport = FakeTransport(self)
        self.clock = Clock()
        self.callLater = self.clock.callLater
        self.fake_sent_pdus = []

    def fake_send_pdu(self, pdu):
        self.fake_sent_pdus.append(pdu)


class FakeEsmeTransceiver(EsmeTransceiver, FakeEsmeMixin):
    def __init__(self, *args, **kwargs):
        EsmeTransceiver.__init__(self, *args, **kwargs)
        self.setup_fake()

    def send_pdu(self, pdu):
        return self.fake_send_pdu(pdu)


class FakeEsmeReceiver(EsmeReceiver, FakeEsmeMixin):
    def __init__(self, *args, **kwargs):
        EsmeReceiver.__init__(self, *args, **kwargs)
        self.setup_fake()

    def send_pdu(self, pdu):
        return self.fake_send_pdu(pdu)


class FakeEsmeTransmitter(EsmeTransmitter, FakeEsmeMixin):
    def __init__(self, *args, **kwargs):
        EsmeTransmitter.__init__(self, *args, **kwargs)
        self.setup_fake()

    def send_pdu(self, pdu):
        return self.fake_send_pdu(pdu)


class EsmeTestCaseBase(VumiTestCase):
    ESME_CLASS = None

    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self._expected_callbacks = []
        self.add_cleanup(
            self.assertEqual, self._expected_callbacks, [],
            "Uncalled callbacks.")

    def get_unbound_esme(self, host="127.0.0.1", port="0",
                         system_id="1234", password="password",
                         callbacks={}, extra_config={}):
        config_data = {
            "transport_name": "transport_name",
            "host": host,
            "port": port,
            "system_id": system_id,
            "password": password,
        }
        config_data.update(extra_config)
        config = SmppTransportConfig(config_data)
        esme_callbacks = EsmeCallbacks(**callbacks)

        def purge_manager(redis_manager):
            d = redis_manager._purge_all()  # just in case
            d.addCallback(lambda result: redis_manager)
            return d

        redis_d = self.persistence_helper.get_redis_manager()
        redis_d.addCallback(purge_manager)
        return redis_d.addCallback(
            lambda r: self.ESME_CLASS(config, {
                'system_id': system_id,
                'password': password
            }, r, esme_callbacks))

    @inlineCallbacks
    def get_esme(self, config={}, **callbacks):
        esme = yield self.get_unbound_esme(extra_config=config,
                                           callbacks=callbacks)
        yield esme.connectionMade()
        esme.fake_sent_pdus.pop()  # Clear bind PDU.
        esme.state = esme.CONNECTED_STATE
        returnValue(esme)

    def get_sm(self, msg, data_coding=3):
        sm = DeliverSM(1, short_message=msg, data_coding=data_coding)
        return unpack_pdu(sm.get_bin())

    def make_cb(self, fun):
        cb_id = len(self._expected_callbacks)
        self._expected_callbacks.append(cb_id)

        def cb(**value):
            self._expected_callbacks.remove(cb_id)
            return fun(value)

        return cb

    def assertion_cb(self, expected, *message_path):
        def fun(value):
            for k in message_path:
                value = value[k]
            self.assertEqual(expected, value)

        return self.make_cb(fun)


class EsmeGenericMixin(object):
    """Generic tests."""

    @inlineCallbacks
    def test_bind_timeout(self):
        callbacks_called = []
        esme = yield self.get_unbound_esme(callbacks={
            'connect': lambda client: callbacks_called.append('connect'),
            'disconnect': lambda: callbacks_called.append('disconnect'),
        })
        yield esme.connectionMade()

        self.assertEqual([], callbacks_called)
        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.clock.advance(esme.smpp_bind_timeout)

        self.assertEqual(['disconnect'], callbacks_called)
        self.assertEqual(False, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)

    @inlineCallbacks
    def test_bind_no_timeout(self):
        callbacks_called = []
        esme = yield self.get_unbound_esme(callbacks={
            'connect': lambda client: callbacks_called.append('connect'),
            'disconnect': lambda: callbacks_called.append('disconnect'),
        })
        yield esme.connectionMade()

        self.assertEqual([], callbacks_called)
        self.assertEqual(True, esme.transport.connected)
        self.assertNotEqual(None, esme._lose_conn)

        esme.handle_bind_transceiver_resp(unpack_pdu(
            BindTransceiverResp(1).get_bin()))

        self.assertEqual(['connect'], callbacks_called)
        self.assertEqual(True, esme.transport.connected)
        self.assertEqual(None, esme._lose_conn)
        esme.lc_enquire.stop()
        yield esme.lc_enquire.deferred

    @inlineCallbacks
    def test_bind_and_disconnect(self):
        callbacks_called = []
        esme = yield self.get_unbound_esme(callbacks={
            'connect': lambda client: callbacks_called.append('connect'),
            'disconnect': lambda: callbacks_called.append('disconnect'),
        })
        yield esme.connectionMade()

        esme.handle_bind_transceiver_resp(unpack_pdu(
            BindTransceiverResp(1).get_bin()))

        self.assertEqual(['connect'], callbacks_called)
        esme.lc_enquire.stop()
        yield esme.lc_enquire.deferred

        yield esme.transport.loseConnection()

        self.assertEqual(['connect', 'disconnect'], callbacks_called)
        self.assertEqual(False, esme.transport.connected)

    @inlineCallbacks
    def test_sequence_rollover(self):
        esme = yield self.get_unbound_esme()
        self.assertEqual(1, (yield esme.get_next_seq()))
        self.assertEqual(2, (yield esme.get_next_seq()))
        yield esme.redis.set('smpp_last_sequence_number', 0xFFFF0000)
        self.assertEqual(0xFFFF0001, (yield esme.get_next_seq()))
        self.assertEqual(1, (yield esme.get_next_seq()))

    @inlineCallbacks
    def test_unbind(self):
        esme = yield self.get_esme()

        yield esme.handle_data(Unbind(1).get_bin())

        self.assertEqual(False, esme.transport.connected)


class EsmeTransmitterMixin(EsmeGenericMixin):
    """Transmitter-side tests."""

    @inlineCallbacks
    def test_submit_sm_sms(self):
        """Submit a USSD message with a session continue flag."""
        esme = yield self.get_esme()
        yield esme.submit_sm(short_message='hello')
        [sm_pdu] = esme.fake_sent_pdus
        sm = unpack_pdu(sm_pdu.get_bin())
        self.assertEqual('submit_sm', sm['header']['command_id'])
        self.assertEqual(
            'hello', sm['body']['mandatory_parameters']['short_message'])
        self.assertEqual([], sm['body'].get('optional_parameters', []))

    @inlineCallbacks
    def test_submit_sm_sms_long(self):
        """Submit a USSD message with a session continue flag."""
        esme = yield self.get_esme(config={
            'send_long_messages': True,
        })
        long_message = 'This is a long message.' * 20
        yield esme.submit_sm(short_message=long_message)
        [sm_pdu] = esme.fake_sent_pdus
        sm = unpack_pdu(sm_pdu.get_bin())
        pdu_opts = unpacked_pdu_opts(sm)

        self.assertEqual('submit_sm', sm['header']['command_id'])
        self.assertEqual(
            None, sm['body']['mandatory_parameters']['short_message'])
        self.assertEqual(''.join('%02x' % ord(c) for c in long_message),
                         pdu_opts['message_payload'])

    @inlineCallbacks
    def test_submit_sm_sms_multipart_sar(self):
        """Submit a long SMS message using multipart sar fields."""
        esme = yield self.get_esme(config={
            'send_multipart_sar': True,
        })
        long_message = 'This is a long message.' * 20
        seq_nums = yield esme.submit_sm(short_message=long_message)
        self.assertEqual([2, 3, 4, 5], seq_nums)
        self.assertEqual(4, len(esme.fake_sent_pdus))
        msg_parts = []
        msg_refs = []

        for i, sm_pdu in enumerate(esme.fake_sent_pdus):
            sm = unpack_pdu(sm_pdu.get_bin())
            pdu_opts = unpacked_pdu_opts(sm)
            mandatory_parameters = sm['body']['mandatory_parameters']

            self.assertEqual('submit_sm', sm['header']['command_id'])
            msg_parts.append(mandatory_parameters['short_message'])
            self.assertTrue(len(mandatory_parameters['short_message']) <= 130)
            msg_refs.append(pdu_opts['sar_msg_ref_num'])
            self.assertEqual(i + 1, pdu_opts['sar_segment_seqnum'])
            self.assertEqual(4, pdu_opts['sar_total_segments'])

        self.assertEqual(long_message, ''.join(msg_parts))
        self.assertEqual(1, len(set(msg_refs)))

    @inlineCallbacks
    def test_submit_sm_sms_multipart_udh(self):
        """Submit a long SMS message using multipart user data headers."""
        esme = yield self.get_esme(config={
            'send_multipart_udh': True,
        })
        long_message = 'This is a long message.' * 20
        seq_nums = yield esme.submit_sm(short_message=long_message)
        self.assertEqual([2, 3, 4, 5], seq_nums)
        self.assertEqual(4, len(esme.fake_sent_pdus))
        msg_parts = []
        msg_refs = []

        for i, sm_pdu in enumerate(esme.fake_sent_pdus):
            sm = unpack_pdu(sm_pdu.get_bin())
            mandatory_parameters = sm['body']['mandatory_parameters']
            self.assertEqual('submit_sm', sm['header']['command_id'])
            msg = mandatory_parameters['short_message']

            udh_hlen, udh_tag, udh_len, udh_ref, udh_tot, udh_seq = [
                ord(octet) for octet in msg[:6]]
            self.assertEqual(5, udh_hlen)
            self.assertEqual(0, udh_tag)
            self.assertEqual(3, udh_len)
            msg_refs.append(udh_ref)
            self.assertEqual(4, udh_tot)
            self.assertEqual(i + 1, udh_seq)
            self.assertTrue(len(msg) <= 136)
            msg_parts.append(msg[6:])
            self.assertEqual(0x40, mandatory_parameters['esm_class'])

        self.assertEqual(long_message, ''.join(msg_parts))
        self.assertEqual(1, len(set(msg_refs)))

    @inlineCallbacks
    def test_submit_sm_ussd_continue(self):
        """Submit a USSD message with a session continue flag."""
        esme = yield self.get_esme()
        yield esme.submit_sm(
            short_message='hello', message_type='ussd', continue_session=True,
            session_info='0100')
        [sm_pdu] = esme.fake_sent_pdus
        sm = unpack_pdu(sm_pdu.get_bin())
        pdu_opts = unpacked_pdu_opts(sm)

        self.assertEqual('submit_sm', sm['header']['command_id'])
        self.assertEqual(
            'hello', sm['body']['mandatory_parameters']['short_message'])
        self.assertEqual('02', pdu_opts['ussd_service_op'])
        self.assertEqual('0100', pdu_opts['its_session_info'])

    @inlineCallbacks
    def test_submit_sm_ussd_close(self):
        """Submit a USSD message with a session close flag."""
        esme = yield self.get_esme()
        yield esme.submit_sm(
            short_message='hello', message_type='ussd', continue_session=False)
        [sm_pdu] = esme.fake_sent_pdus
        sm = unpack_pdu(sm_pdu.get_bin())
        pdu_opts = unpacked_pdu_opts(sm)

        self.assertEqual('submit_sm', sm['header']['command_id'])
        self.assertEqual(
            'hello', sm['body']['mandatory_parameters']['short_message'])
        self.assertEqual('02', pdu_opts['ussd_service_op'])
        self.assertEqual('0001', pdu_opts['its_session_info'])


class EsmeReceiverMixin(EsmeGenericMixin):
    """Receiver-side tests."""

    @inlineCallbacks
    def test_deliver_sm_simple(self):
        """A simple message should be delivered."""
        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(u'hello', 'short_message'))
        yield esme.handle_deliver_sm(self.get_sm('hello'))

    @inlineCallbacks
    def test_deliver_sm_message_payload(self):
        """A message in the `message_payload` field should be delivered."""
        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(u'hello', 'short_message'))
        sm = DeliverSM(1, short_message='')
        sm.add_message_payload(''.join('%02x' % ord(c) for c in 'hello'))
        yield esme.handle_deliver_sm(unpack_pdu(sm.get_bin()))

    @inlineCallbacks
    def test_deliver_sm_data_coding_override(self):
        """A simple message should be delivered."""
        esme = yield self.get_esme(config={
            'data_coding_overrides': {
                0: 'utf-16be'
            }
        }, deliver_sm=self.assertion_cb(u'hello', 'short_message'))

        yield esme.handle_deliver_sm(
            self.get_sm('\x00h\x00e\x00l\x00l\x00o', 0))

        esme = yield self.get_esme(config={
            'data_coding_overrides': {
                0: 'ascii'
            }
        }, deliver_sm=self.assertion_cb(u'hello', 'short_message'))
        yield esme.handle_deliver_sm(
            self.get_sm('hello', 0))

    @inlineCallbacks
    def test_deliver_sm_ucs2(self):
        """A UCS-2 message should be delivered."""
        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(u'hello', 'short_message'))
        yield esme.handle_deliver_sm(
            self.get_sm('\x00h\x00e\x00l\x00l\x00o', 8))

    @inlineCallbacks
    def test_bad_sm_ucs2(self):
        """An invalid UCS-2 message should be discarded."""
        bad_msg = '\n\x00h\x00e\x00l\x00l\x00o'

        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(bad_msg, 'short_message'))

        yield esme.handle_deliver_sm(self.get_sm(bad_msg, 8))
        self.flushLoggedErrors()

    @inlineCallbacks
    def test_deliver_sm_delivery_report_delivered(self):
        esme = yield self.get_esme(delivery_report=self.assertion_cb({
            'message_id': '1b1720be-5f48-41c4-b3f8-6e59dbf45366',
            'message_state': 'DELIVERED',
        }))

        sm = DeliverSM(1, short_message='delivery report')
        sm._PDU__add_optional_parameter(
            'receipted_message_id',
            '1b1720be-5f48-41c4-b3f8-6e59dbf45366')
        sm._PDU__add_optional_parameter('message_state', 2)

        yield esme.handle_deliver_sm(unpack_pdu(sm.get_bin()))

    @inlineCallbacks
    def test_deliver_sm_delivery_report_rejected(self):
        esme = yield self.get_esme(delivery_report=self.assertion_cb({
            'message_id': '1b1720be-5f48-41c4-b3f8-6e59dbf45366',
            'message_state': 'REJECTED',
        }))

        sm = DeliverSM(1, short_message='delivery report')
        sm._PDU__add_optional_parameter(
            'receipted_message_id',
            '1b1720be-5f48-41c4-b3f8-6e59dbf45366')
        sm._PDU__add_optional_parameter('message_state', 8)

        yield esme.handle_deliver_sm(unpack_pdu(sm.get_bin()))

    @inlineCallbacks
    def test_deliver_sm_delivery_report_regex_fallback(self):
        esme = yield self.get_esme(delivery_report=self.assertion_cb({
            'message_id': '1b1720be-5f48-41c4-b3f8-6e59dbf45366',
            'message_state': 'DELIVRD',
        }))

        yield esme.handle_deliver_sm(self.get_sm(
                'id:1b1720be-5f48-41c4-b3f8-6e59dbf45366 sub:001 dlvrd:001 '
                'submit date:120726132548 done date:120726132548 stat:DELIVRD '
                'err:000 text:'))

    @inlineCallbacks
    def test_deliver_sm_delivery_report_regex_fallback_ucs2(self):
        esme = yield self.get_esme(delivery_report=self.assertion_cb({
            'message_id': '1b1720be-5f48',
            'message_state': 'DELIVRD',
        }))

        dr_text = (
            u'id:1b1720be-5f48 sub:001 dlvrd:001 '
            u'submit date:120726132548 done date:120726132548 stat:DELIVRD '
            u'err:000 text:').encode('utf-16be')
        yield esme.handle_deliver_sm(self.get_sm(dr_text, 8))

    @inlineCallbacks
    def test_deliver_sm_delivery_report_regex_fallback_ucs2_long(self):
        esme = yield self.get_esme(delivery_report=self.assertion_cb({
            'message_id': '1b1720be-5f48-41c4-b3f8-6e59dbf45366',
            'message_state': 'DELIVRD',
        }))

        dr_text = (
            u'id:1b1720be-5f48-41c4-b3f8-6e59dbf45366 sub:001 dlvrd:001 '
            u'submit date:120726132548 done date:120726132548 stat:DELIVRD '
            u'err:000 text:').encode('utf-16be')
        sm = DeliverSM(1, short_message='', data_coding=8)
        sm.add_message_payload(dr_text.encode('hex'))
        yield esme.handle_deliver_sm(unpack_pdu(sm.get_bin()))

    @inlineCallbacks
    def test_deliver_sm_multipart(self):
        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(u'hello world', 'short_message'))
        yield esme.handle_deliver_sm(self.get_sm(
                "\x05\x00\x03\xff\x02\x02 world"))
        yield esme.handle_deliver_sm(self.get_sm(
                "\x05\x00\x03\xff\x02\x01hello"))

    @inlineCallbacks
    def test_deliver_sm_multipart_weird_coding(self):
        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(u'hello', 'short_message'))
        yield esme.handle_deliver_sm(self.get_sm(
                "\x05\x00\x03\xff\x02\x02l\x00l\x00o", 8))
        yield esme.handle_deliver_sm(self.get_sm(
                "\x05\x00\x03\xff\x02\x01\x00h\x00e\x00", 8))

    @inlineCallbacks
    def test_deliver_sm_multipart_arabic_ucs2(self):
        esme = yield self.get_esme(
            deliver_sm=self.assertion_cb(
                ('\xd8\xa7\xd9\x84\xd9\x84\xd9\x87 '
                 '\xd9\x85\xd8\xb9\xd9\x83').decode('utf-8'), 'short_message'),
            config={
                'data_coding_overrides': {
                    8: 'utf-8'
                }
            })
        yield esme.handle_deliver_sm(self.get_sm(
            "\x05\x00\x03\xff\x02\x01\xd8\xa7\xd9\x84\xd9\x84\xd9\x87 ", 8))
        yield esme.handle_deliver_sm(self.get_sm(
            "\x05\x00\x03\xff\x02\x02\xd9\x85\xd8\xb9\xd9\x83", 8))

    @inlineCallbacks
    def test_deliver_sm_ussd_start(self):
        def assert_ussd(value):
            self.assertEqual('ussd', value['message_type'])
            self.assertEqual('new', value['session_event'])
            self.assertEqual(None, value['short_message'])

        esme = yield self.get_esme(deliver_sm=self.make_cb(assert_ussd))

        sm = DeliverSM(1)
        sm._PDU__add_optional_parameter('ussd_service_op', '01')
        sm._PDU__add_optional_parameter('its_session_info', '0000')

        yield esme.handle_deliver_sm(unpack_pdu(sm.get_bin()))


class TestEsmeTransceiver(EsmeTestCaseBase, EsmeReceiverMixin,
                          EsmeTransmitterMixin):
    ESME_CLASS = FakeEsmeTransceiver


class TestEsmeTransmitter(EsmeTestCaseBase, EsmeTransmitterMixin):
    ESME_CLASS = FakeEsmeTransmitter

    @inlineCallbacks
    def test_deliver_sm_simple(self):
        """A message delivery should log an error since we're supposed
        to be a transmitter only."""
        def cb(**kw):
            self.assertEqual(u'hello', kw['short_message'])

        with LogCatcher() as log:
            esme = yield self.get_esme(deliver_sm=cb)
            esme.state = 'BOUND_TX'  # Assume we've bound correctly as a TX
            esme.handle_deliver_sm(self.get_sm('hello'))
            [error] = log.errors
            self.assertTrue('deliver_sm in wrong state' in error['message'][0])


class TestEsmeReceiver(EsmeTestCaseBase, EsmeReceiverMixin):
    ESME_CLASS = FakeEsmeReceiver

    @inlineCallbacks
    def test_submit_sm_simple(self):
        """A simple message log an error when trying to send over
        a receiver."""
        with LogCatcher() as log:
            esme = yield self.get_esme()
            esme.state = 'BOUND_RX'  # Fake RX bind
            yield esme.submit_sm(short_message='hello')
            [error] = log.errors
            self.assertTrue(('submit_sm in wrong state' in
                             error['message'][0]))


class TestESME(VumiTestCase):

    def setUp(self):
        config = SmppTransportConfig({
            "transport_name": "transport_name",
            "host": '127.0.0.1',
            "port": 2775,
            "system_id": 'test_system',
            "password": 'password',
        })
        self.kvs = None
        self.esme_callbacks = None
        self.esme = ESME(config, {
            'system_id': 'test_system',
            'password': 'password',
        }, self.kvs, self.esme_callbacks)

    def test_bind_as_transceiver(self):
        return self.esme.bindTransciever()
