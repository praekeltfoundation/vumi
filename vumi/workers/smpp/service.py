



class SmppService(Worker):
    """
    The SmppService
    """

    def startWorker(self):
        log.msg("Starting the SmppService")
        # start the Smpp transport
        factory = SmscServerFactory()
        reactor.connectTCP(
                factory.defaults['host'],
                factory.defaults['port'],
                factory)


    def stopWorker(self):
        log.msg("Stopping the SMPPService")

