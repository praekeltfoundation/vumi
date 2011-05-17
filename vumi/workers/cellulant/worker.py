import re

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor

from vumi.service import Worker
from vumi.message import Message

class XMPPtoCellulantUSSDWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPtoCellulantUSSDWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.inbound.cellulant.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.inbound.gtalk.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        SESSIONID = 'sessionID'
        NETWORKID = 'networkID'
        MSISDN = message.payload['sender']
        MESSAGE = message.payload['message']
        OPERATION = 'INVA'
        message = "%s|%s|%s|%s|%s" % (
                SESSIONID,
                NETWORKID,
                MSISDN,
                MESSAGE,
                OPERATION)
        self.publisher.publish_message(Message(message=message))

    def stopWorker(self):
        log.msg("Stopping the XMPPtoCellulantUSSDWorker")



class CellulantUSSDtoXMPPWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the CellulantUSSDtoXMPPWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.outbound.cellulant.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        mess = re.search(
                  '(?P<SESSIONID>^[^|]*)'
                +'|(?P<NETWORKID>[^|]*)'
                +'|(?P<MSISDN>[^|]*)'
                +'|(?P<MESSAGE>[^|]*)'
                +'|(?P<OPERATION>[^|]*$)',
                message.payload['short_message'])
        self.publisher.publish_message(Message(
            recipient=mess.groupdict()['MSISDN'],
            message=mess.groupdict()['MESSAGE']))

    def stopWorker(self):
        log.msg("Stopping the CellulantUSSDtoXMPPWorker")

