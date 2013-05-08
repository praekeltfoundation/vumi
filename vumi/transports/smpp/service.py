
from twisted.python import log
from twisted.internet import reactor

from vumi.service import Worker
from vumi.transports.smpp.clientserver.server import SmscServerFactory


class SmppService(Worker):
    """
    The SmppService
    """

    def startWorker(self):
        log.msg("Starting the SmppService")

        delivery_report_string = self.config.get('smsc_delivery_report_string')
        self.factory = SmscServerFactory(
            delivery_report_string=delivery_report_string)
        self.listening = reactor.listenTCP(self.config['port'], self.factory)
