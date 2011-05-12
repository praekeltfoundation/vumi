from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from datetime import datetime

from vumi.workers.smpp.transport import SmppTransport

class TopicSmppTransport(SmppTransport):
    
    @inlineCallbacks
    def esme_connected(self, client):
        log.msg("ESME Connected, adding handlers")
        self.esme_client = client
        self.esme_client.set_handler(self)

        # Start the publisher
        self.publisher = yield self.publish_to(
            routing_key = 'sms.inbound.%s' % self.config.get('TRANSPORT_NAME'),
            exchange_name = self.config.get('exchange_name'),
            exchange_type = self.config.get('exchange_type')
        )
        
        # Match on a wildcard
        self.consume(
            routing_key = 'sms.outbound.%s.*' % self.config.get('TRANSPORT_NAME'),
            callback = self.handle_outbound_sms,
            exchange_name = self.config.get('exchange_name'),
            exchange_type = self.config.get('exchange_type')
        )
    
    def handle_outbound_sms(self, message):
        # send the message via SMPP
        sequence_number = self.send_smpp(**message.payload)
        # get the sequence_number, link it back to our own internal
        formdict = {
                "sent_sms":message.payload.get("id"),
                "sequence_number": sequence_number,
                }
        log.msg("SMPPLinkForm", repr(formdict))
        form = forms.SMPPLinkForm(formdict)
        form.save()
        return True
