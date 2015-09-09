from twisted.internet.defer import Deferred, succeed, DeferredQueue
from twisted.internet.interfaces import IStreamClientEndpoint
from twisted.internet.protocol import Protocol
from twisted.protocols.loopback import loopbackAsync
from zope.interface import implementer

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiverResp, BindTransmitterResp, BindReceiverResp)
from vumi.transports.smpp.pdu_utils import seq_no, chop_pdu_stream


class FakeSMSC(object):
    """
    Fake SMSC for testing.
    """
    def __init__(self, auto_accept=True, auto_bind=True):
        self.auto_accept = auto_accept
        self.auto_bind = auto_bind
        self.pdu_queue = DeferredQueue()
        self.endpoint = FakeSMSCEndpoint(self)
        self.protocol = None
        self._reset_connection_ds()

    # Public API.

    def await_connecting(self):
        """
        Wait for a client to start connecting.

        This is useful if auto-accept is disabled, otherwise use
        :meth:`await_connected` instead.
        """
        return self._listen_d

    def await_connected(self):
        """
        Wait for a client to finish connecting.
        """
        return self._connected_d

    def accept_connection(self):
        """
        Accept a pending connection.

        This is only useful is auto-accept is disabled.
        """
        self._accept_d.callback(self.protocol)

    def send_pdu(self, pdu):
        """
        Send a PDU to the connected ESME.
        """
        self.protocol.send_pdu(pdu)

    def bind(self, bind_pdu=None):
        """
        Respond to a bind command.

        :param bind_pdu:
            The bind PDU to respond to. If `None`, the next PDU on the receive
            queue will be used.
        """
        if bind_pdu is not None:
            bind_d = succeed(bind_pdu)
        else:
            bind_d = self.pdu_queue.get()
        return bind_d.addCallback(self._bind_resp)

    def await_pdu(self):
        """
        Wait for the next PDU for the receive queue.
        """
        return self.pdu_queue.get()

    # Internal stuff.

    def _reset_connection_ds(self):
        self._listen_d = Deferred()
        self._accept_d = Deferred()
        self._connected_d = Deferred()
        self._finished_d = Deferred()
        self._client_protocol = None

    def handle_connection(self, client_protocol):
        assert self.protocol is None
        self._client_protocol = client_protocol
        self.protocol = FakeSMSCProtocol(self)
        self._listen_d.callback(None)
        if self.auto_accept:
            self.accept_connection()
        return self._accept_d

    def connection_made(self):
        self._connected_d.callback(None)
        if self.auto_bind:
            self.bind()

    def pdu_received(self, pdu):
        self.pdu_queue.put(pdu)

    def set_finished(self, finished_d):
        finished_d.addCallback(self.finished_d.callback)

    def _bind_resp(self, bind_pdu):
        resp_pdu_class = {
            'bind_transceiver': BindTransceiverResp,
            'bind_receiver': BindReceiverResp,
            'bind_transmitter': BindTransmitterResp,
        }.get(bind_pdu['header']['command_id'])
        self.send_pdu(resp_pdu_class(seq_no(bind_pdu)))
        return bind_pdu


@implementer(IStreamClientEndpoint)
class FakeSMSCEndpoint(object):
    """
    This endpoint connects a client directly to a FakeSMSC.
    """
    def __init__(self, fake_smsc):
        self.fake_smsc = fake_smsc

    def connect(self, protocolFactory):
        client = protocolFactory.buildProtocol(None)
        d = self.fake_smsc.handle_connection(client)
        return d.addCallback(self._make_connection, client)

    def _make_connection(self, server, client):
        finished_d = loopbackAsync(server, client)
        self.fake_smsc.set_finished(finished_d)
        return client


class FakeSMSCProtocol(Protocol):
    """
    Very simple protocol for pretending to be an SMSC.
    """

    def __init__(self, fake_smsc):
        self.fake_smsc = fake_smsc
        self._buf = b""

    def connectionMade(self):
        self.fake_smsc.connection_made()

    def dataReceived(self, data):
        self._buf += data
        data = self.handle_buffer()
        while data is not None:
            self.pdu_received(unpack_pdu(data))
            data = self.handle_buffer()

    def handle_buffer(self):
        pdu_found = chop_pdu_stream(self._buf)
        if pdu_found is None:
            return

        data, self._buf = pdu_found
        return data

    def process_pdus(self):
        pdu_found = chop_pdu_stream(self._buf)
        while pdu_found is not None:
            pdu_data, self._buf = pdu_found
            self.pdu_received(unpack_pdu(pdu_data))
            pdu_found = chop_pdu_stream(self._buf)

    def pdu_received(self, pdu):
        self.fake_smsc.pdu_received(pdu)

    def send_pdu(self, pdu):
        self.transport.write(pdu.get_bin())
