from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker
from vumi.message import Message


class XMPPWorker(Worker):

    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPWorker config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' %
                                                self.config['username'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("xmpp.inbound.gtalk.%s" % self.config['username'],
                        self.consume_message)

    def consume_message(self, message):
        recipient = message.payload['sender']
        message = "You said: %s " % message.payload['message']
        self.publisher.publish_message(Message(recipient=recipient,
                                               message=message))

    def stopWorker(self):
        log.msg("Stopping the XMPPWorker")
