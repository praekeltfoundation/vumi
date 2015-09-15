# -*- test-case-name: vumi.transports.smpp.tests.test_fake_smsc -*-

from twisted.internet.defer import (
    Deferred, succeed, DeferredQueue, gatherResults)
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.interfaces import IStreamClientEndpoint
from twisted.internet.protocol import Protocol
from twisted.internet.task import deferLater
from twisted.protocols.loopback import loopbackAsync
from zope.interface import implementer

from smpp.pdu import unpack_pdu
from smpp.pdu_builder import (
    BindTransceiverResp, BindTransmitterResp, BindReceiverResp,
    EnquireLinkResp, UnbindResp, DeliverSM, SubmitSMResp)
from vumi.transports.smpp.pdu_utils import seq_no, chop_pdu_stream, command_id


def wait0(r=None):
    """
    Wait zero seconds to give the reactor a chance to work.

    Returns its (optional) argument, so it's useful as a callback.
    """
    from twisted.internet import reactor
    return deferLater(reactor, 0, lambda: r)


class FakeSMSC(object):
    """
    Fake SMSC for testing.

    By default, it accepts incoming connections and automatically responds to
    unbind commands. Only one client connection at a time is allowed.
    """
    def __init__(self, auto_accept=True, auto_unbind=True):
        self.auto_accept = auto_accept
        self.auto_unbind = auto_unbind
        self.pdu_queue = DeferredQueue()
        self.endpoint = FakeSMSCEndpoint(self)
        self.connected = False
        self._reset_connection_ds()

    # Public API.

    def await_connecting(self):
        """
        Wait for a client to start connecting, and then return the client
        protocol.

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

        This is only useful if auto-accept is disabled.
        """
        assert self.has_pending_connection(), "No pending connection."
        self._accept_d.callback(self.protocol)
        return self.await_connected()

    def reject_connection(self):
        """
        Reject a pending connection.

        This is only useful if auto-accept is disabled. The deferred returned
        by waiting for `await_connected()` for this connection will never fire.
        """
        assert self.has_pending_connection(), "No pending connection."
        self._accept_d.errback(ConnectionRefusedError())
        self._reset_connection_ds()

    def has_pending_connection(self):
        """
        Returns `True` if there is a pending connection, `False` otherwise.
        """
        return self._accept_d is not None and not self._accept_d.called

    def send_bytes(self, bytes):
        """
        Put some bytes on the wire.

        This also waits zero seconds to allow the bytes to be delivered.
        """
        self.protocol.transport.write(bytes)
        return wait0()

    def send_pdu(self, pdu):
        """
        Send a PDU to the connected ESME.

        This also waits zero seconds to allow the PDU to be delivered.
        """
        self.protocol.send_pdu(pdu)
        return wait0()

    def handle_pdu(self, pdu):
        """
        Bypass the wire connection and call `on_pdu` directly.

        This allows the caller to wait until the PDU processing has finished.
        It also allows invalid PDUs to be sent.
        """
        return self._client_protocol.on_pdu(pdu.obj)

    def bind(self, bind_pdu=None):
        """
        Respond to a bind command.

        :param bind_pdu:
            The bind PDU to respond to. If `None`, the next PDU on the receive
            queue will be used.
        """
        bind_d = self._given_or_next_pdu(bind_pdu)
        return bind_d.addCallback(self._bind_resp)

    def respond_to_enquire_link(self, enquire_link_pdu=None):
        """
        Respond to an enquire_link command.

        :param enquire_link_pdu:
            The enquire_link PDU to respond to. If `None`, the next PDU on the
            receive queue will be used.
        """
        enquire_link_d = self._given_or_next_pdu(enquire_link_pdu)
        return enquire_link_d.addCallback(self._enquire_link_resp)

    def await_pdu(self):
        """
        Wait for the next PDU from the receive queue.
        """
        return self.pdu_queue.get()

    def await_pdus(self, count):
        """
        Wait for the next `count` PDUs from the receive queue.
        """
        return gatherResults([self.pdu_queue.get() for _ in range(count)])

    def waiting_pdu_count(self):
        """
        Returns the number of PDUs in the receive queue.
        """
        return len(self.pdu_queue.pending)

    def send_mo(self, sequence_number, short_message, data_coding=1, **kwargs):
        """
        Send a DeliverSM PDU.
        """
        return self.send_pdu(
            DeliverSM(
                sequence_number, short_message=short_message,
                data_coding=data_coding, **kwargs))

    def submit_sm_resp(self, submit_sm_pdu=None, message_id=None, **kw):
        """
        Respond to a submit_sm command.

        NOTE: This uses :meth:`handle_pdu` instead of :meth:`send_pdu` because
              there's a lot of async stuff going on.

        :param submit_sm_pdu:
            The submit_sm PDU to respond to. If `None`, the next PDU on the
            receive queue will be used.
        :param message_id:
            The message_id to put in the response. If `None`, one will be
            generated from the sequence number.
        """
        submit_sm_d = self._given_or_next_pdu(submit_sm_pdu)
        return submit_sm_d.addCallback(self._submit_sm_resp, message_id, **kw)

    def disconnect(self):
        """
        Disconnect.
        """
        self.protocol.transport.loseConnection()
        return self.await_disconnect()

    def await_disconnect(self):
        """
        Wait for the client to disconnect.
        """
        return self._finished_d

    # Internal stuff.

    def _reset_connection_ds(self):
        # self._finished_d is special, because we need that after the
        # connection gets closed.
        self._listen_d = Deferred()
        self._accept_d = None
        self._connected_d = Deferred()
        self._bound_d = Deferred()
        self._client_protocol = None
        self.protocol = None

    def handle_connection(self, client_protocol):
        assert self.protocol is None, "Already connected."
        self._client_protocol = client_protocol
        self.protocol = FakeSMSCProtocol(self)
        self._accept_d = Deferred()
        self._listen_d.callback(client_protocol)
        if self.auto_accept:
            self.accept_connection()
        return self._accept_d

    def connection_made(self):
        self.connected = True
        self._connected_d.callback(None)

    def connection_lost(self):
        self.connected = False
        self.protocol.transport.loseConnection()
        self._reset_connection_ds()

    def pdu_received(self, pdu):
        self.pdu_queue.put(pdu)
        if self.auto_unbind and command_id(pdu) == 'unbind':
            self.send_pdu(UnbindResp(seq_no(pdu)))

    def set_finished(self, finished_d):
        self._finished_d = Deferred()
        finished_d.addCallback(self._finished_d.callback)

    def _given_or_next_pdu(self, pdu):
        if pdu is not None:
            return succeed(pdu)
        return self.pdu_queue.get()

    def assert_command_id(self, pdu, *command_ids):
        if command_id(pdu) not in command_ids:
            raise ValueError(
                "Expected PDU with command_id in [%s], got %s." % (
                    ", ".join(command_ids), command_id(pdu)))

    def _bind_resp(self, bind_pdu):
        resp_pdu_classes = {
            'bind_transceiver': BindTransceiverResp,
            'bind_receiver': BindReceiverResp,
            'bind_transmitter': BindTransmitterResp,
        }
        self.assert_command_id(bind_pdu, *resp_pdu_classes)
        resp_pdu_class = resp_pdu_classes.get(command_id(bind_pdu))
        self.send_pdu(resp_pdu_class(seq_no(bind_pdu)))
        eq_d = self.respond_to_enquire_link()
        return eq_d.addCallback(self._bound_d.callback)

    def _enquire_link_resp(self, enquire_link_pdu):
        self.assert_command_id(enquire_link_pdu, 'enquire_link')
        return self.send_pdu(EnquireLinkResp(seq_no(enquire_link_pdu)))

    def _submit_sm_resp(self, submit_sm_pdu, message_id, **kw):
        self.assert_command_id(submit_sm_pdu, 'submit_sm')
        sequence_number = seq_no(submit_sm_pdu)
        if message_id is None:
            message_id = "id%s" % (sequence_number,)
        # We use handle_pdu here to avoid complications with all the async.
        return self.handle_pdu(SubmitSMResp(sequence_number, message_id, **kw))


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

    def connectionLost(self, reason):
        self.fake_smsc.connection_lost()

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

    def pdu_received(self, pdu):
        self.fake_smsc.pdu_received(pdu)

    def send_pdu(self, pdu):
        self.transport.write(pdu.get_bin())
