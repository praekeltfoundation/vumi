# -*- test-case-name: vumi.middleware.tests.test_manhole -*-

from vumi.middleware import BaseMiddleware

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.cred import portal
from twisted.conch import manhole_ssh, manhole_tap
from twisted.conch.checkers import SSHPublicKeyDatabase
from twisted.python.filepath import FilePath
from twisted.internet.endpoints import serverFromString


class SSHPubKeyDatabase(SSHPublicKeyDatabase):
    """
    Checker for authorizing people against a list of `authorized_keys` files.
    If nothing is specified then it defaults to `authorized_keys` and
    `authorized_keys2` for the logged in user.
    """
    def __init__(self, authorized_keys):
        self.authorized_keys = authorized_keys

    def getAuthorizedKeysFiles(self, credentials):
        if self.authorized_keys is not None:
            return [FilePath(ak) for ak in self.authorized_keys]

        return SSHPublicKeyDatabase.getAuthorizedKeysFiles(self, credentials)


class ManholeMiddleware(BaseMiddleware):
    """
    Middleware providing SSH access into the worker this middleware is attached
    to.

    Requires the following packages to be installed:

        * pyasn1
        * pycrypto


    :param str twisted_endpoint:
        The Twisted endpoint to listen on.
        Defaults to `tcp:0` which has the reactor select any available port.
    :param list authorized_keys:
        List of absolute paths to `authorized_keys` files containing SSH public
        keys that are allowed access.
    """
    def validate_config(self):
        self.twisted_endpoint = self.config.get('twisted_endpoint', 'tcp:0')
        self.authorized_keys = self.config.get('authorized_keys', None)

    @inlineCallbacks
    def setup_middleware(self):
        self.validate_config()
        checker = SSHPubKeyDatabase(self.authorized_keys)
        ssh_realm = manhole_ssh.TerminalRealm()
        ssh_realm.chainedProtocolFactory = manhole_tap.chainedProtocolFactory({
            'worker': self.worker,
        })
        ssh_portal = portal.Portal(ssh_realm, [checker])
        factory = manhole_ssh.ConchFactory(ssh_portal)
        endpoint = serverFromString(reactor, self.twisted_endpoint)
        self.socket = yield endpoint.listen(factory)

    def teardown_middleware(self):
        return self.socket.stopListening()
