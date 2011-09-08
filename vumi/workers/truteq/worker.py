from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.message import Message
from vumi.service import Worker, Consumer, Publisher
from vumi.workers.truteq.util import SessionType


class TruTeqConsumer(Consumer):
    """
    This consumer consumes all incoming USSD messages on the *120*663*79#
    shortcode in a campaign specific queue.
    """
    exchange_name = "vumi.ussd"
    exchange_type = "topic"
    durable = False
    queue_name = "ussd.truteq.test_campaign"
    routing_key = "ussd.s120s663s79h"

    def __init__(self, publisher):
        self.publisher = publisher

    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        dictionary = message.payload
        ussd_type = dictionary.get('ussd_type')
        msisdn = dictionary.get('msisdn')
        message = dictionary.get('message')
        if(ussd_type == SessionType.NEW):
            response = {
                'ussd_type': SessionType.EXISTING,
                'msisdn': msisdn,
                'message': "Hi! This is an echo service",
            }
        else:
            response = {
                'ussd_type': SessionType.END,
                'msisdn': msisdn,
                'message': 'You said: %s' % message,
            }
        self.publisher.publish_message(Message(**response))
        return True


class TruTeqPublisher(Publisher):
    exchange_name = "vumi.ussd"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'ussd.truteq.s120s663s79h'
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s" % (message,
                                                               kwargs))
        super(TruTeqPublisher, self).publish_message(message, **kwargs)


class USSDWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the USSDWorker")
        self.publisher = yield self.start_publisher(TruTeqPublisher)
        self.consumer = yield self.start_consumer(TruTeqConsumer,
                                                  self.publisher)

    def stopWorker(self):
        log.msg("Stopping the USSDWorker")
