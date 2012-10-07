# -*- test-case-name: vumi.middleware.tests.test_manhole -*-

from vumi.middleware import BaseMiddleware

from twisted.internet import reactor
from twisted.cred import portal
from twisted.conch import manhole_ssh, manhole_tap
from twisted.conch.checkers import SSHPublicKeyDatabase
from twisted.python.filepath import FilePath


class SSHPubKeyDatabase(SSHPublicKeyDatabase):
    """
    Checker for authorizing people against a list of `authorized_keys` files.
    If nothing is specified then it defaults to `authorized_keys` and
    `authorized_keys2` for the logged in user.
    """
    def __init__(self, authorized_keys):
        self.authorized_keys = authorized_keys

    def getAuthorizedKeysFiles(self, credentials):
        if self.authorized_keys:
            return [FilePath(ak) for ak in self.authorized_keys]

        return SSHPublicKeyDatabase.getAuthorizedKeysFiles(self, credentials)


class ManholeMiddleware(BaseMiddleware):
    """
    Middleware providing SSH access into the worker this middleware is attached
    to.

    :param int port:
        The port to open up. Defaults to `0` which has the reactor select
        any available port.
    :param list authorized_keys:
        List of absolute paths to `authorized_keys` files containing SSH public
        keys that are allowed access.
    """
    def validate_config(self):
        self.port = int(self.config.get('port', 0))
        self.authorized_keys = self.config.get('authorized_keys', [])

    def setup_middleware(self):
        self.validate_config()
        checker = SSHPubKeyDatabase(self.authorized_keys)
        sshRealm = manhole_ssh.TerminalRealm()
        sshRealm.chainedProtocolFactory = manhole_tap.chainedProtocolFactory({
            'worker': self.worker,
            })
        sshPortal = portal.Portal(sshRealm, [checker])
        sshFactory = manhole_ssh.ConchFactory(sshPortal)
        self.socket = reactor.listenTCP(self.port, sshFactory)

    def teardown_middleware(self):
        self.socket.loseConnection()
