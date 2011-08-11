from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.message import Message
from vumi.service import Worker, Consumer, Publisher


class FoneworxConsumer(Consumer):
    exchange_name = "vumi.sms"
    exchange_type = "topic"
    durable = False
    queue_name = "sms.foneworx.test_campaign"
    routing_key = "sms.foneworx.test_campaign"

    def __init__(self, publisher):
        self.publisher = publisher
        self.storage = []

    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        response = {
            'msisdn': message.payload['msisdn'],
            'message': "You said: %s" % message.payload['message']
        }
        self.publisher.publish_message(Message(**response))
        return True


class FoneworxPublisher(Publisher):
    exchange_name = "vumi.sms"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'sms.foneworx.test_campaign'
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s" % (
                message, kwargs))
        super(FoneworxPublisher, self).publish_message(message, **kwargs)


class SMSWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSWorker")
        self.publisher = yield self.start_publisher(FoneworxPublisher)
        self.consumer = yield self.start_consumer(FoneworxConsumer,
                                                  self.publisher)

    def stopWorker(self):
        log.msg("Stopping the SMSWorker")
