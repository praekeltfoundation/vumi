import os

from tempfile import NamedTemporaryFile

from twisted.trial.unittest import TestCase
from twisted.conch.manhole_ssh import ConchFactory
from twisted.conch.ssh import (transport, userauth, connection, channel,
    session)
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
        return self.requestService(ClientUserAuth(os.getlogin(),
            ClientConnection(self.factory.channelConnected)))


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

    @inlineCallbacks
    def setUp(self):

        self.private_key_file = NamedTemporaryFile(mode='w')
        self.private_key_file.write(private_key.toString('OPENSSH'))
        self.private_key_file.flush()

        self.pub_key_file = NamedTemporaryFile(mode='w')
        self.pub_key_file.write(public_key.toString('OPENSSH'))
        self.pub_key_file.flush()

        self._middlewares = []

        mw = self.get_middleware({
            'authorized_keys': [self.pub_key_file.name]
        })
        host = mw.socket.getHost()

        factory = protocol.ClientFactory()
        factory.protocol = ClientTransport

        wait_for_channel = defer.Deferred()

        factory.channelConnected = wait_for_channel
        self.socket = reactor.connectTCP(host.host, host.port, factory)

        self.channel = yield self.open_shell(wait_for_channel)

    @inlineCallbacks
    def open_shell(self, wait_for_channel):
        channel = yield wait_for_channel
        conn = channel.conn
        term = session.packRequest_pty_req("xterm-mono", (0, 0, 0, 0), '')
        yield conn.sendRequest(channel, 'pty-req', term, wantReply=1)
        yield conn.sendRequest(channel, 'shell', '', wantReply=1)
        defer.returnValue(channel)

    def tearDown(self):
        for mw in self._middlewares:
            mw.teardown_middleware()
        self.socket.disconnect()

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
        self.channel.write('print worker.transport_name\n')
        sent_line = yield self.channel.queue.get()
        received_line = yield self.channel.queue.get()
        self.assertEqual(received_line, 'foo')
