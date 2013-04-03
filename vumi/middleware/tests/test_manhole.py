from tempfile import NamedTemporaryFile

from twisted.trial.unittest import TestCase
from twisted.conch.manhole_ssh import ConchFactory
from twisted.conch import error
from twisted.conch.ssh import (transport, userauth, connection, channel,
    common)
from twisted.internet import defer, protocol, reactor
from twisted.internet.endpoints import TCP4ClientEndpoint
from twisted.internet.defer import inlineCallbacks

from vumi.middleware.manhole import ManholeMiddleware


# these are shipped along with Twisted
private_key = ConchFactory.privateKeys['ssh-rsa']
public_key = ConchFactory.publicKeys['ssh-rsa']


class ClientTransport(transport.SSHClientTransport):

    def verifyHostKey(self, pubKey, fingerprint):
        if fingerprint != 'b1:94:6a:c9:24:92:d2:34:7c:62:35:b4:d2:61:11:84':
            return defer.fail(error.ConchError('bad key'))
        else:
            return defer.succeed(1)

    def connectionSecure(self):
        self.requestService(ClientUserAuth('user', ClientConnection()))


class ClientUserAuth(userauth.SSHUserAuthClient):

    def getPassword(self, prompt=None):
        return
        # this says we won't do password authentication

    def getPublicKey(self):
        return public_key.blob()

    def getPrivateKey(self):
        return defer.succeed(private_key.keyObject)


class ClientConnection(connection.SSHConnection):

    def serviceStarted(self):
        self.openChannel(CatChannel(conn=self))


class CatChannel(channel.SSHChannel):

    name = 'session'

    # def channelOpen(self, data):
    #     d = self.conn.sendRequest(self, 'exec', common.NS('cat'),
    #                               wantReply=1)
    #     d.addCallback(self._cbSendRequest)
    #     self.catData = ''

    # def _cbSendRequest(self, ignored):
    #     self.write('This data will be echoed back to us by "cat."\r\n')
    #     self.conn.sendEOF(self)
    #     self.loseConnection()

    def dataReceived(self, data):
        self.catData += data

    def closed(self):
        print 'We got this from "cat":', self.catData


class ManholeMiddlewareTestCase(TestCase):

    def setUp(self):

        self.private_key_file = NamedTemporaryFile()
        self.private_key_file.write(private_key.toString('OPENSSH'))

        self.pub_key_file = NamedTemporaryFile()
        self.pub_key_file.write(public_key.toString('OPENSSH'))

        self._middlewares = []

    def tearDown(self):
        for mw in self._middlewares:
            mw.teardown_middleware()

    def get_middleware(self, config={}):
        config = dict({
            'port': '0',
        }, **config)
        worker = object()
        mw = ManholeMiddleware("test_manhole_mw", config, worker)
        mw.setup_middleware()
        self._middlewares.append(mw)
        return mw

    @inlineCallbacks
    def test_mw(self):
        mw = self.get_middleware()
        host = mw.socket.getHost()

        factory = protocol.ClientFactory()
        factory.protocol = ClientTransport

        point = TCP4ClientEndpoint(reactor, host.host, host.port)
        proto = yield point.connect(factory)
        print proto
