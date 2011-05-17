from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker
from vumi.message import Message

class XMPPtoCellulantUSSDWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.inbound.cellulant.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.inbound.gtalk.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        recipient = message.payload['sender']
        message = "You said: %s " % message.payload['message']
        self.publisher.publish_message(Message(recipient=recipient, message=message))

    def stopWorker(self):
        log.msg("Stopping the XMPPWorker")



class CellulantUSSDtoXMPPWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.outbound.cellulant.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        recipient = message.payload['sender']
        message = "You said: %s " % message.payload['message']
        self.publisher.publish_message(Message(recipient=recipient, message=message))

    def stopWorker(self):
        log.msg("Stopping the XMPPWorker")

