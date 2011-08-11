from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from datetime import datetime

from foneworx.client import Client, TwistedConnection
from vumi.service import Worker, Consumer, Publisher


class FoneworxConsumer(Consumer):
    exchange_name = "vumi.sms"
    exchange_type = "topic"
    durable = False
    queue_name = "sms.foneworx"
    routing_key = "sms.foneworx.*"

    def __init__(self):
        # queue = []
        pass

    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        self.queue.append(message)
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


class SMSTransport(Worker):
    """
    The SMSTransport for Foneworx
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSTransport")

        self.last_polled_at = None

        username = self.config.pop('username')
        password = self.config.pop('password')
        host = self.config.pop("host")
        port = self.config.pop("port")

        self.client = Client(username, password,
                                connection=TwistedConnection(
                                    host,
                                    port,
                                ))
        self.publisher = yield self.start_publisher(FoneworxPublisher)
        self.consumer = yield self.start_consumer(FoneworxConsumer)
        reactor.callLater(0, self.send_and_receive)

    @inlineCallbacks
    def send_and_receive(self):
        log.msg("Sending and receiving")
        new_messages = self.receive(self.last_polled_at)
        self.last_polled_at = datetime.now()  # this is inaccurate
        for inbound in new_messages:
            self.publisher.publish_message(inbound)
            self.delete(inbound)
        for outbound in self.consumer.queue:
            self.send(**outbound)

    @inlineCallbacks
    def send(self, msisdn, message):
        sent_messages = yield self.client.send_messages([
            {
                'msisdn': msisdn,
                'message': message
            }
        ])
        returnValue(sent_messages)

    @inlineCallbacks
    def receive(self, *args, **kwargs):
        new_messages = yield self.client.new_messages(*args, **kwargs)
        returnValue(new_messages)

    @inlineCallbacks
    def delete(self, sms):
        yield self.client.delete_message(sms['sms_id'])

    def stopWorker(self):
        log.msg("Stopping the SMSTransport")
