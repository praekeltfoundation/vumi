import re

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker
from vumi.message import Message


def packCellulantUSSDMessage(message):
    SESSIONID = '1A3E55B'
    NETWORKID = '3'
    MSISDN = message.payload['sender']
    MESSAGE = message.payload['message']
    OPERATION = 'INVA'
    mess = "%s|%s|%s|%s|%s" % (
            SESSIONID,
            NETWORKID,
            MSISDN,
            MESSAGE,
            OPERATION)
    return Message(message=mess)


def unpackCellulantUSSDMessage(message):
    mess = re.search(
              '^(?P<SESSIONID>[^|]*)'
            + '\|(?P<NETWORKID>[^|]*)'
            + '\|(?P<MSISDN>[^|]*)'
            + '\|(?P<MESSAGE>[^|]*)'
            + '\|(?P<OPERATION>[^|]*)$',
            message.payload['message'])
    return Message(recipient=mess.groupdict()['MSISDN'],
                    message=mess.groupdict()['MESSAGE'])


class XMPPtoCellulantUSSDWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPtoCellulantUSSDWorker config: %s" % (
                self.config,))
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.inbound.cellulant.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.inbound.gtalk.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        _message = packCellulantUSSDMessage(message)
        try:
            self.publisher.publish_message(_message)
        except:
            pass
        return _message

    def stopWorker(self):
        log.msg("Stopping the XMPPtoCellulantUSSDWorker")


class CellulantUSSDtoXMPPWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the CellulantUSSDtoXMPPWorker config: %s" % (
                self.config,))
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.outbound.cellulant.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        _message = unpackCellulantUSSDMessage(message)
        try:
            self.publisher.publish_message(_message)
        except:
            pass
        return _message

    def stopWorker(self):
        log.msg("Stopping the CellulantUSSDtoXMPPWorker")
