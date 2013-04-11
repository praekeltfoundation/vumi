import os

from twisted.conch.ssh import transport, userauth, connection, channel
from twisted.internet import defer

from twisted.conch.manhole_ssh import ConchFactory

# these are shipped along with Twisted
private_key = ConchFactory.privateKeys['ssh-rsa']
public_key = ConchFactory.publicKeys['ssh-rsa']


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
