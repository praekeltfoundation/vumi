from twisted.trial.unittest import SkipTest

from twisted.internet import defer, protocol, reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.endpoints import TCP4ClientEndpoint

from vumi.tests.helpers import VumiTestCase

try:

    from twisted.conch.manhole_ssh import ConchFactory
    from twisted.conch.ssh import session
    from vumi.middleware.manhole import ManholeMiddleware
    from vumi.middleware.manhole_utils import ClientTransport

    # these are shipped along with Twisted
    private_key = ConchFactory.privateKeys['ssh-rsa']
    public_key = ConchFactory.publicKeys['ssh-rsa']

except ImportError:
    ssh = False
else:
    ssh = True


class DummyWorker(object):
    pass


class TestManholeMiddleware(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        if not ssh:
            raise SkipTest('Crypto requirements missing. Skipping Test.')

        self.pub_key_file_name = self.mktemp()
        self.pub_key_file = open(self.pub_key_file_name, 'w')
        self.pub_key_file.write(public_key.toString('OPENSSH'))
        self.pub_key_file.flush()

        self.mw = yield self.get_middleware({
            'authorized_keys': [self.pub_key_file.name]
        })

    @inlineCallbacks
    def open_shell(self, middleware):
        host = middleware.socket.getHost()
        factory = protocol.ClientFactory()
        factory.protocol = ClientTransport
        factory.channelConnected = defer.Deferred()

        endpoint = TCP4ClientEndpoint(reactor, host.host, host.port)
        transport = yield endpoint.connect(factory)

        channel = yield factory.channelConnected
        conn = channel.conn
        term = session.packRequest_pty_req("vt100", (0, 0, 0, 0), '')
        yield conn.sendRequest(channel, 'pty-req', term, wantReply=1)
        yield conn.sendRequest(channel, 'shell', '', wantReply=1)
        self.add_cleanup(transport.loseConnection)
        defer.returnValue(channel)

    @inlineCallbacks
    def get_middleware(self, config={}):
        config = dict({
            'port': '0',
        }, **config)

        worker = DummyWorker()
        worker.transport_name = 'foo'

        mw = ManholeMiddleware("test_manhole_mw", config, worker)
        yield mw.setup_middleware()
        self.add_cleanup(mw.teardown_middleware)
        returnValue(mw)

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
