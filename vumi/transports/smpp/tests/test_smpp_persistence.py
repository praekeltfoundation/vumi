
from twisted.internet import defer

from vumi.tests.utils import FakeRedis
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.smpp.tests.test_smpp import (
        RedisTestEsmeTransceiver,
        RedisTestSmppTransport)


class MessagePayloadPersistenceTestCase(TransportTestCase):

    transport_name = "redis_testing_transport"
    transport_class = RedisTestSmppTransport

    @defer.inlineCallbacks
    def setUp(self):
        super(MessagePayloadPersistenceTestCase, self).setUp()
        self.seq = [123456]
        self.config = {
                "system_id": "vumitest-vumitest-vumitest",
                "host": "host",
                "port": "port",
                "smpp_increment": 10,
                "smpp_offset": 6,
                "TRANSPORT_NAME": "redis_testing_transport",
                }
        self.vumi_options = {
                "vhost": "develop",
                }

        # hack a lot of transport setup
        self.esme = RedisTestEsmeTransceiver(
                self.seq, self.config, self.vumi_options)
        self.esme.state = 'BOUND_TRX'
        self.transport = yield self.get_transport(self.config, start=False)
        self.transport.esme_client = self.esme
        self.transport.r_server = FakeRedis()
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

    def test_pass(self):
        pass
