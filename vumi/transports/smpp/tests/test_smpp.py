import redis

from twisted.internet import defer
from twisted.trial.unittest import TestCase
from smpp.pdu_builder import SubmitSMResp, BindTransceiverResp

from vumi.tests.utils import FakeRedis
from vumi.message import Message
from vumi.transports.smpp.clientserver.client import (
        EsmeTransceiver,
        ESME,
        KeyValueStore)
from vumi.transports.smpp.clientserver.tests.test_client import (
        KeyValueStoreTestCase)
from vumi.transports.smpp.transport import SmppTransport
from vumi.transports.smpp.clientserver.config import ClientConfig
from vumi.transports.tests.test_base import TransportTestCase


class EsmeClientInitTestcase(TestCase):

    def test_esme_init_with_redis(self):
        r_server = redis.Redis("localhost", db=13)
        #print dir(r_server)
        #print r_server.delete("vumi.transports.smpp.tests.test_smpp#counter")
        #print r_server.keys()
        self.esme = ESME(
                None,
                r_server,
                )
        kvstc = KeyValueStoreTestCase()
        kvstc.prefix = __name__
        kvstc.run_all_tests_on_instance(self.esme.kvs)

    def test_esme_init_with_fakeredis(self):
        fake_redis = FakeRedis()
        self.esme = ESME(
                None,
                fake_redis,
                )
        kvstc = KeyValueStoreTestCase()
        kvstc.prefix = __name__
        kvstc.run_all_tests_on_instance(self.esme.kvs)

    def test_esme_init_with_simple_keyvaluestore(self):
        key_value_store = KeyValueStore()
        self.esme = ESME(
                None,
                key_value_store,
                )
        kvstc = KeyValueStoreTestCase()
        kvstc.prefix = __name__
        kvstc.run_all_tests_on_instance(self.esme.kvs)
        self.assertTrue(key_value_store.is_empty())

    def test_esme_init_with_bad_object(self):
        key_value_store = self
        self.esme = ESME(
                None,
                key_value_store,
                )
        kvstc = KeyValueStoreTestCase()
        kvstc.prefix = __name__
        exception_expected = None
        try:
            kvstc.run_all_tests_on_instance(self.esme.kvs)
            self.assertTrue(key_value_store.is_empty())
        except Exception, e:
            exception_expected = e
        self.assertTrue(exception_expected is not None)


class RedisTestEsmeTransceiver(EsmeTransceiver):

    def sendPDU(self, pdu):
        pass  # don't actually send anything


class RedisTestSmppTransport(SmppTransport):

    def send_smpp(self, message):
        to_addr = message['to_addr']
        text = message['content']
        sequence_number = self.esme_client.submit_sm(
                short_message=text.encode('utf-8'),
                destination_addr=str(to_addr),
                source_addr="1234567890",
                )
        return sequence_number

    # TODO remove these fault methods to bring
    # failure handling more in line with other transports
    def ok(self, *args, **kwargs):
        pass

    def mess_permfault(self, *args, **kwargs):
        pass

    def mess_tempfault(self, *args, **kwargs):
        pass

    def conn_permfault(self, *args, **kwargs):
        pass

    def conn_tempfault(self, *args, **kwargs):
        pass

    def conn_throttle(self, *args, **kwargs):
        if kwargs.get('pdu'):
            self.throttle_invoked_via_pdu = True


class FakeRedisRespTestCase(TransportTestCase):

    transport_name = "redis_testing_transport"
    transport_class = RedisTestSmppTransport

    @defer.inlineCallbacks
    def setUp(self):
        super(FakeRedisRespTestCase, self).setUp()
        self.config = {
                "TRANSPORT_NAME": "redis_testing_transport",
                "system_id": "vumitest-vumitest-vumitest",
                "host": "host",
                "port": "port",
                "password": "password",
                }
        self.vumi_options = {
                "vhost": "develop",
                }
        self.clientConfig = ClientConfig(
                system_id=self.config['system_id'],
                host=self.config['host'],
                port=self.config['port'],
                password=self.config['password'],
                )


        # hack a lot of transport setup
        self.transport = yield self.get_transport(self.config, start=False)
        self.transport.r_server = FakeRedis()
        self.esme = RedisTestEsmeTransceiver(
                self.clientConfig,
                self.transport.r_server)
        self.esme.state = 'BOUND_TRX'
        self.transport.esme_client = self.esme
        self.esme.setSubmitSMRespCallback(self.transport.submit_sm_resp)

        # set error handlers
        self.esme.update_error_handlers({
            "ok": self.transport.ok,
            "mess_permfault": self.transport.mess_permfault,
            "mess_tempfault": self.transport.mess_tempfault,
            "conn_permfault": self.transport.conn_permfault,
            "conn_tempfault": self.transport.conn_tempfault,
            "conn_throttle": self.transport.conn_throttle,
            })

        yield self.transport.startWorker()
        self.transport.esme_connected(self.esme)

    def test_redis_message_persistence(self):
        # A simple test of set -> get -> delete for redis message persistence
        message1 = self.mkmsg_out(
            message_id='1234567890abcdefg',
            content="hello world",
            to_addr="far-far-away")
        original_json = message1.to_json()
        self.transport.r_set_message(message1)
        retrieved_json = self.transport.r_get_message_json(
                message1.payload['message_id'])
        self.assertEqual(original_json, retrieved_json)
        retrieved_message = self.transport.r_get_message(
                message1.payload['message_id'])
        self.assertEqual(retrieved_message, message1)
        self.assertTrue(self.transport.r_delete_message(
            message1.payload['message_id']))
        self.assertEqual(self.transport.r_get_message_json(
            message1.payload['message_id']), None)
        self.assertEqual(self.transport.r_get_message(
            message1.payload['message_id']), None)

    @defer.inlineCallbacks
    def test_match_resp(self):
        message1 = self.mkmsg_out(
            message_id='444',
            content="hello world",
            to_addr="1111111111")
        sequence_num1 = self.esme.getSeq()
        response1 = SubmitSMResp(sequence_num1, "3rd_party_id_1")
        yield self.transport._process_message(message1)

        message2 = self.mkmsg_out(
            message_id='445',
            content="hello world",
            to_addr="1111111111")
        sequence_num2 = self.esme.getSeq()
        response2 = SubmitSMResp(sequence_num2, "3rd_party_id_2")
        yield self.transport._process_message(message2)

        # respond out of order - just to keep things interesting
        self.esme.handleData(response2.get_bin())
        self.esme.handleData(response1.get_bin())

        self.assertEqual([
                self.mkmsg_ack('445', '3rd_party_id_2'),
                self.mkmsg_ack('444', '3rd_party_id_1'),
                ], self.get_dispatched_events())

        message3 = self.mkmsg_out(
            message_id='446',
            content="hello world",
            to_addr="1111111111")
        sequence_num3 = self.esme.getSeq()
        response3 = SubmitSMResp(sequence_num3, "3rd_party_id_3",
                command_status="ESME_RSUBMITFAIL")
        self.transport._process_message(message3)
        self.esme.handleData(response3.get_bin())
        # There should be no ack
        self.assertEqual([], self.get_dispatched_events()[2:])

        comparison = self.mkmsg_fail(message3.payload, 'ESME_RSUBMITFAIL')
        actual = self.get_dispatched_failures()[0]
        self.assertEqual(actual, comparison)

        message4 = self.mkmsg_out(
            message_id=447,
            content="hello world",
            to_addr="1111111111")
        sequence_num4 = self.esme.getSeq()
        response4 = SubmitSMResp(sequence_num4, "3rd_party_id_4",
                command_status="ESME_RTHROTTLED")
        self.transport._process_message(message4)
        self.esme.handleData(response4.get_bin())
        # There should be no ack
        self.assertEqual([], self.get_dispatched_events()[3:])
        self.assertTrue(self.transport.throttle_invoked_via_pdu)

        comparison = self.mkmsg_fail(message4.payload, 'ESME_RTHROTTLED')
        actual = self.get_dispatched_failures()[1]
        self.assertEqual(actual, comparison)

        # Some error codes would occur on bind attempts
        bind_dispatch_methods = {
            "ESME_ROK": self.transport.ok,
            "ESME_RINVBNDSTS": self.transport.conn_tempfault,
            "ESME_RALYBND": self.transport.conn_tempfault,
            "ESME_RSYSERR": self.transport.conn_permfault,
            "ESME_RBINDFAIL": self.transport.conn_permfault,
            "ESME_RINVPASWD": self.transport.conn_permfault,
            "ESME_RINVSYSID": self.transport.conn_permfault,
            "ESME_RINVSERTYP": self.transport.conn_permfault,
        }

        # Some error codes would occur post bind i.e. on submission attempts
        submit_dispatch_methods = {
            "ESME_RINVMSGLEN": self.transport.mess_permfault,
            "ESME_RINVCMDLEN": self.transport.mess_permfault,
            "ESME_RINVCMDID": self.transport.mess_permfault,

            "ESME_RINVPRTFLG": self.transport.mess_permfault,
            "ESME_RINVREGDLVFLG": self.transport.mess_permfault,

            "ESME_RINVSRCADR": self.transport.mess_permfault,
            "ESME_RINVDSTADR": self.transport.mess_permfault,
            "ESME_RINVMSGID": self.transport.mess_permfault,

            "ESME_RCANCELFAIL": self.transport.mess_permfault,
            "ESME_RREPLACEFAIL": self.transport.mess_permfault,

            "ESME_RMSGQFUL": self.transport.conn_throttle,

            "ESME_RINVNUMDESTS": self.transport.mess_permfault,
            "ESME_RINVDLNAME": self.transport.mess_permfault,
            "ESME_RINVDESTFLAG": self.transport.mess_permfault,
            "ESME_RINVSUBREP": self.transport.mess_permfault,
            "ESME_RINVESMCLASS": self.transport.mess_permfault,
            "ESME_RCNTSUBDL": self.transport.mess_permfault,

            "ESME_RSUBMITFAIL": self.transport.mess_tempfault,

            "ESME_RINVSRCTON": self.transport.mess_permfault,
            "ESME_RINVSRCNPI": self.transport.mess_permfault,
            "ESME_RINVDSTTON": self.transport.mess_permfault,
            "ESME_RINVDSTNPI": self.transport.mess_permfault,
            "ESME_RINVSYSTYP": self.transport.conn_permfault,
            "ESME_RINVREPFLAG": self.transport.mess_permfault,

            "ESME_RINVNUMMSGS": self.transport.mess_tempfault,

            "ESME_RTHROTTLED": self.transport.conn_throttle,

            "ESME_RINVSCHED": self.transport.mess_permfault,
            "ESME_RINVEXPIRY": self.transport.mess_permfault,
            "ESME_RINVDFTMSGID": self.transport.mess_permfault,

            "ESME_RX_T_APPN": self.transport.mess_tempfault,

            "ESME_RX_P_APPN": self.transport.mess_permfault,
            "ESME_RX_R_APPN": self.transport.mess_permfault,
            "ESME_RQUERYFAIL": self.transport.mess_permfault,
            "ESME_RINVOPTPARSTREAM": self.transport.mess_permfault,
            "ESME_ROPTPARNOTALLWD": self.transport.mess_permfault,
            "ESME_RINVPARLEN": self.transport.mess_permfault,
            "ESME_RMISSINGOPTPARAM": self.transport.mess_permfault,
            "ESME_RINVOPTPARAMVAL": self.transport.mess_permfault,

            "ESME_RDELIVERYFAILURE": self.transport.mess_tempfault,
            "ESME_RUNKNOWNERR": self.transport.mess_tempfault,
        }

        # Also have unknown error codes
        newfangled_fake_error = {
            "ESME_NEWFNGLEDFAKERR": self.esme.dummy_unknown,
        }

        for code, method in bind_dispatch_methods.items():
            response = BindTransceiverResp(1, code)
            # check the dispatcher returns the correct transport method
            self.assertEquals(method,
                    self.esme.command_status_dispatch(response.get_obj()))

        for code, method in submit_dispatch_methods.items():
            response = SubmitSMResp(1, "2", code)
            # check the dispatcher returns the correct transport method
            self.assertEquals(method,
                    self.esme.command_status_dispatch(response.get_obj()))

        for code, method in newfangled_fake_error.items():
            response = SubmitSMResp(1, "2", code)
            # check the dispatcher returns the correct transport method
            self.assertEquals(method,
                    self.esme.command_status_dispatch(response.get_obj()))

    @defer.inlineCallbacks
    def test_reconnect(self):
        connected_chan_count = len(self._amqp.channels)
        disconnected_chan_count = connected_chan_count - 1

        yield self.transport.esme_disconnected()
        self.assertEqual(disconnected_chan_count, len(self._amqp.channels))
        yield self.transport.esme_disconnected()
        self.assertEqual(disconnected_chan_count, len(self._amqp.channels))

        yield self.transport.esme_connected(self.esme)
        self.assertEqual(connected_chan_count, len(self._amqp.channels))
        yield self.transport.esme_connected(self.esme)
        self.assertEqual(connected_chan_count, len(self._amqp.channels))
