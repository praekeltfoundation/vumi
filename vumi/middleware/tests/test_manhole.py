import os

from tempfile import NamedTemporaryFile

from twisted.trial.unittest import TestCase
from twisted.conch.manhole_ssh import ConchFactory
from twisted.conch.ssh import (
    transport, userauth, connection, channel, session)
from twisted.internet import defer, protocol, reactor
from twisted.internet.defer import inlineCallbacks

from vumi.middleware.manhole import ManholeMiddleware


# these are shipped along with Twisted
private_key = ConchFactory.privateKeys['ssh-rsa']
public_key = ConchFactory.publicKeys['ssh-rsa']


class DummyWorker(object):
    pass


class ClientTransport(transport.SSHClientTransport):

    def verifyHostKey(self, pub_key, fingerprint):
        return defer.succeed(1)

    def connectionSecure(self):
        return self.requestService(ClientUserAuth(
            os.getlogin(), ClientConnection(self.factory.channelConnected)))


class ClientUserAuth(userauth.SSHUserAuthClient):

    def getPassword(self, prompt=None):
        # Not doing password based auth
        return

    def getPublicKey(self):
        return public_key.blob()

    def getPrivateKey(self):
        return defer.succeed(private_key.keyObject)


class ClientConnection(connection.SSHConnection):

    def __init__(self, channel_connected):
        connection.SSHConnection.__init__(self)
        self._channel_connected = channel_connected

    def serviceStarted(self):
        channel = ClientChannel(self._channel_connected,
                                conn=self)
        self.openChannel(channel)


class ClientChannel(channel.SSHChannel):

    name = 'session'

    def __init__(self, channel_connected, *args, **kwargs):
        channel.SSHChannel.__init__(self, *args, **kwargs)
        self._channel_connected = channel_connected
        self.buffer = u''
        self.queue = defer.DeferredQueue()

    def channelOpen(self, data):
        self._channel_connected.callback(self)

    def dataReceived(self, data):
        self.buffer += data
        lines = self.buffer.split('\r\n')
        for line in lines[:-1]:
            self.queue.put(line)
        self.buffer = lines[-1]


class ManholeMiddlewareTestCase(TestCase):

    def setUp(self):
        self.pub_key_file_name = self.mktemp()
        self.pub_key_file = open(self.pub_key_file_name, 'w')
        self.pub_key_file.write(public_key.toString('OPENSSH'))
        self.pub_key_file.flush()

        self._middlewares = []
        self._client_sockets = []

        self.mw = self.get_middleware({
            'authorized_keys': [self.pub_key_file.name]
        })

    @inlineCallbacks
    def open_shell(self, middleware):
        host = middleware.socket.getHost()
        factory = protocol.ClientFactory()
        factory.protocol = ClientTransport
        factory.channelConnected = defer.Deferred()

        socket = reactor.connectTCP(host.host, host.port, factory)

        channel = yield factory.channelConnected
        conn = channel.conn
        term = session.packRequest_pty_req("vt100", (0, 0, 0, 0), '')
        yield conn.sendRequest(channel, 'pty-req', term, wantReply=1)
        yield conn.sendRequest(channel, 'shell', '', wantReply=1)
        self._client_sockets.append(socket)
        defer.returnValue(channel)

    def tearDown(self):
        for socket in self._client_sockets:
            socket.disconnect()

        for mw in self._middlewares:
            mw.teardown_middleware()

    def get_middleware(self, config={}):
        config = dict({
            'port': '0',
        }, **config)

        worker = DummyWorker()
        worker.transport_name = 'foo'

        mw = ManholeMiddleware("test_manhole_mw", config, worker)
        mw.setup_middleware()
        self._middlewares.append(mw)
        return mw

    @inlineCallbacks
    def test_mw(self):
        shell = yield self.open_shell(self.mw)
        shell.write('print worker.transport_name\n')
        # read the echoed line we sent first, this is hard to test because
        # I'm not seeing how I can tell Twisted not to use color in the
        # returned response.
        yield shell.queue.get()
        # next is the server response
        received_line = yield shell.queue.get()
        self.assertEqual(received_line, 'foo')
