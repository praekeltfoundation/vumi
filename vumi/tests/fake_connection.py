from twisted.internet.defer import Deferred, DeferredQueue
from twisted.internet.error import (
    ConnectionRefusedError, ConnectionDone, ConnectionAborted)
from twisted.internet.interfaces import IStreamClientEndpoint
from twisted.internet.protocol import Protocol, ServerFactory
from twisted.internet.task import deferLater
from twisted.protocols.loopback import loopbackAsync
from twisted.python.failure import Failure
from twisted.web.client import ProxyAgent
from twisted.web.resource import Resource
from twisted.web.server import Site
from zope.interface import implementer


def wait0(r=None):
    """
    Wait zero seconds to give the reactor a chance to work.

    Returns its (optional) argument, so it's useful as a callback.
    """
    from twisted.internet import reactor
    return deferLater(reactor, 0, lambda: r)


class ProtocolDouble(Protocol):
    """
    A stand-in protocol for manually driving one side of a connection.
    """

    def __init__(self):
        self.received = b""
        self.disconnected_reason = None

    def dataReceived(self, data):
        self.received += data

    def connectionLost(self, reason):
        self.connected = False
        self.disconnected_reason = reason

    def write(self, data):
        """
        Write some bytes and allow the reactor to send them.
        """
        self.transport.write(data)
        return wait0()


class FakeServer(object):
    """
    Fake server container for testing client/server interactions.
    """

    def __init__(self, server_factory, auto_accept=True, on_connect=None):
        self.server_factory = server_factory
        self.auto_accept = auto_accept
        self.connection_queue = DeferredQueue()
        self.on_connect = on_connect

    # Public API.

    @classmethod
    def for_protocol(cls, protocol, *args, **kw):
        factory = ServerFactory.forProtocol(protocol)
        return cls(factory, *args, **kw)

    @property
    def endpoint(self):
        """
        Get an endpoint that connects clients to this server.
        """
        return FakeServerEndpoint(self)

    def await_connection(self):
        """
        Wait for a client to start connecting, and then return a
        :class:`FakeConnection` object.
        """
        return self.connection_queue.get()

    # Internal stuff.

    def _handle_connection(self):
        conn = FakeConnection(self)
        if self.on_connect is not None:
            conn._connected_d.addCallback(lambda _: self.on_connect(conn))
        self.connection_queue.put(conn)
        if self.auto_accept:
            conn.accept_connection()
        return conn._accept_d


class FakeConnection(object):
    """
    Fake server connection.
    """

    def __init__(self, server):
        self.server = server
        self.client_protocol = None
        self.server_protocol = None

        self._accept_d = Deferred()
        self._connected_d = Deferred()
        self._finished_d = Deferred()

    @property
    def connected(self):
        return self._connected_d.called and not self._finished_d.called

    @property
    def pending(self):
        return not self._accept_d.called

    def await_connected(self):
        """
        Wait for a client to finish connecting.
        """
        return self._connected_d

    def accept_connection(self):
        """
        Accept a pending connection.
        """
        assert self.pending, "Connection is not pending."
        self.server_protocol = self.server.server_factory.buildProtocol(None)
        self._accept_d.callback(
            FakeServerProtocolWrapper(self, self.server_protocol))
        return self.await_connected()

    def reject_connection(self, reason=None):
        """
        Reject a pending connection.
        """
        assert self.pending, "Connection is not pending."
        if reason is None:
            reason = ConnectionRefusedError()
        self._accept_d.errback(reason)

    def await_finished(self):
        """
        Wait for the both sides of the connection to close.
        """
        return self._finished_d

    # Internal stuff.

    def _finish_connecting(self, client_protocol, finished_d):
        self.client_protocol = client_protocol
        finished_d.chainDeferred(self._finished_d)
        self._connected_d.callback(None)


@implementer(IStreamClientEndpoint)
class FakeServerEndpoint(object):
    """
    This endpoint connects a client directly to a FakeSMSC.
    """
    def __init__(self, server):
        self.server = server

    def connect(self, protocolFactory):
        d = self.server._handle_connection()
        return d.addCallback(self._make_connection, protocolFactory)

    def _make_connection(self, server, protocolFactory):
        client = protocolFactory.buildProtocol(None)
        patch_protocol_for_agent(client)
        finished_d = loopbackAsync(server, client)
        server.finish_connecting(client, finished_d)
        return client


def patch_protocol_for_agent(protocol):
    """
    Patch the protocol's makeConnection and connectionLost methods to make the
    protocol and its transport behave more like what `Agent` expects.

    While `Agent` is the driving force behind this, other clients and servers
    will no doubt have similar requirements.
    """
    old_makeConnection = protocol.makeConnection
    old_connectionLost = protocol.connectionLost

    def new_makeConnection(transport):
        patch_transport_fake_push_producer(transport)
        patch_transport_abortConnection(transport, protocol)
        return old_makeConnection(transport)

    def new_connectionLost(reason):
        # Replace ConnectionDone with ConnectionAborted if we aborted.
        if protocol._fake_connection_aborted and reason.check(ConnectionDone):
            reason = Failure(ConnectionAborted())
        return old_connectionLost(reason)

    protocol.makeConnection = new_makeConnection
    protocol.connectionLost = new_connectionLost
    protocol._fake_connection_aborted = False


def patch_if_missing(obj, name, method):
    """
    Patch a method onto an object if it isn't already there.
    """
    setattr(obj, name, getattr(obj, name, method))


def patch_transport_fake_push_producer(transport):
    """
    Patch the three methods belonging to IPushProducer onto the transport if it
    doesn't already have them. (`Agent` assumes its transport has these.)
    """
    patch_if_missing(transport, 'pauseProducing', lambda: None)
    patch_if_missing(transport, 'resumeProducing', lambda: None)
    patch_if_missing(transport, 'stopProducing', transport.loseConnection)


def patch_transport_abortConnection(transport, protocol):
    """
    Patch abortConnection() on the transport or add it if it doesn't already
    exist (`Agent` assumes its transport has this).

    The patched method sets an internal flag recording the abort and then calls
    the original method (if it existed) or transport.loseConnection (if it
    didn't).
    """
    _old_abortConnection = getattr(
        transport, 'abortConnection', transport.loseConnection)

    def abortConnection():
        protocol._fake_connection_aborted = True
        _old_abortConnection()

    transport.abortConnection = abortConnection


class FakeServerProtocolWrapper(Protocol):
    """
    Wrapper around a server protocol to track connection state.
    """

    def __init__(self, connection, protocol):
        self.connection = connection
        patch_protocol_for_agent(protocol)
        self.protocol = protocol

    def makeConnection(self, transport):
        Protocol.makeConnection(self, transport)
        return self.protocol.makeConnection(transport)

    def connectionLost(self, reason):
        return self.protocol.connectionLost(reason)

    def dataReceived(self, data):
        return self.protocol.dataReceived(data)

    def finish_connecting(self, client, finished_d):
        self.connection._finish_connecting(client, finished_d)


class FakeHttpServer(object):
    """
    HTTP server built on top of FakeServer.
    """

    def __init__(self, handler):
        site_factory = Site(HandlerResource(handler))
        self.fake_server = FakeServer(site_factory)

    @property
    def endpoint(self):
        return self.fake_server.endpoint

    def get_agent(self, reactor=None, contextFactory=None):
        """
        Returns an IAgent that makes requests to this fake server.
        """
        return ProxyAgentWithContext(
            self.endpoint, reactor=reactor, contextFactory=contextFactory)


class HandlerResource(Resource):
    isLeaf = True

    def __init__(self, handler):
        Resource.__init__(self)
        self.handler = handler

    def render_GET(self, request):
        return self.handler(request)

    def render_POST(self, request):
        return self.handler(request)

    def render_PUT(self, request):
        return self.handler(request)


class ProxyAgentWithContext(ProxyAgent):
    def __init__(self, endpoint, reactor=None, contextFactory=None):
        self.contextFactory = contextFactory  # To assert on in tests.
        super(ProxyAgentWithContext, self).__init__(endpoint, reactor=reactor)
