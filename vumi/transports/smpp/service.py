
from twisted.python import log
from twisted.internet import reactor

from vumi.service import Worker, Consumer, Publisher
from vumi.transports.smpp.clientserver.server import SmscServerFactory


class SmppService(Worker):
    """
    The SmppService
    """
    test_hook = None

    def set_test_hook(self, test_hook):
        self.test_hook = test_hook

    def startWorker(self):
        log.msg("Starting the SmppService")
        # start the Smpp Service
        self.factory = SmscServerFactory(test_hook=self.test_hook)
        self.listening = reactor.listenTCP(self.config['port'], self.factory)
