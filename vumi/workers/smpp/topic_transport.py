from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.transports.smpp.transport import SmppTransport


class TopicSmppTransport(SmppTransport):

    @inlineCallbacks
    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        self.esme_client.set_handler(self)

        # Start the publisher
        self.publisher = yield self.publish_to(
            routing_key='sms.inbound.%s' %
                         self.config.get('TRANSPORT_NAME'),
            exchange_name=self.config.get('exchange_name'),
            exchange_type=self.config.get('exchange_type'),
        )

        # Match on a wildcard
        self.consume(
            routing_key='sms.outbound.%s.*' %
                        self.config.get('TRANSPORT_NAME'),
            callback=self.handle_outbound_sms,
            exchange_name=self.config.get('exchange_name'),
            exchange_type=self.config.get('exchange_type'),
        )

    def handle_outbound_sms(self, message):
        # send the message via SMPP
        self.consume_message(message)
        return True
