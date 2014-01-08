from twisted.test import proto_helpers
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.error import ConnectionDone
from twisted.application.service import Service

from vumi.tests.helpers import VumiTestCase
from vumi.transports.tests.helpers import TransportHelper

from vumi.transports.smpp.smpp_transport import SmppTransport
from vumi.transports.smpp.clientserver.tests.test_new_client import (
    bind_protocol, wait_for_pdus)


class DummyService(Service):

    def __init__(self, factory):
        self.factory = factory
        self.protocol = None

    def startService(self):
        self.protocol = self.factory.buildProtocol(('120.0.0.1', 0))

    def stopService(self):
        if self.protocol and self.protocol.transport:
            self.protocol.transport.loseConnection()
            self.protocol.connectionLost(reason=ConnectionDone)


class TestSmppTransport(VumiTestCase):

    def setUp(self):
        self.patch(SmppTransport, 'start_service', self.patched_start_service)

        self.string_transport = proto_helpers.StringTransport()

        self.tx_helper = self.add_helper(TransportHelper(SmppTransport))
        self.default_config = {
            'transport_name': self.tx_helper.transport_name,
            'twisted_endpoint': 'tcp:host=127.0.0.1:port=0',
            'smpp_config': {
                'system_id': 'foo',
                'password': 'bar',
            }
        }

    def patched_start_service(self, factory):
        service = DummyService(factory)
        service.startService()
        return service

    @inlineCallbacks
    def get_transport(self, config={}, bind=True):
        cfg = self.default_config.copy()
        cfg.update(config)
        transport = yield self.tx_helper.get_transport(cfg)
        if bind:
            yield self.create_smpp_bind(transport)
        returnValue(transport)

    def create_smpp_bind(self, smpp_transport):
        d = Deferred()

        def cb(smpp_transport):
            protocol = smpp_transport.service.protocol
            if protocol is None:
                # Still setting up factory
                reactor.callLater(0, cb, smpp_transport)
                return

            if not protocol.isBound():
                # Factory setup, needs bind pdus
                protocol.makeConnection(self.string_transport)
                bind_d = bind_protocol(self.string_transport, protocol)
                bind_d.addCallback(
                    lambda _: reactor.callLater(0, cb, smpp_transport))
                return bind_d

            d.callback(smpp_transport)

        cb(smpp_transport)
        return d

    def sendPDU(self, pdu):
        self.protocol.dataReceived(pdu.get_bin())

    def wait_for_pdus(self, count):
        return wait_for_pdus(self.string_transport, count)

    @inlineCallbacks
    def test_something(self):
        transport = yield self.get_transport()
        print transport
