from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.message import Message
from vumi.webapp.api import utils

class TopicSmppWorker(Worker):
    
    POST_TO_URL = 'http://localhost/sms/receiver'
    
    @inlineCallbacks
    def startWorker(self):
        # create the publisher
        self.publisher = yield self.publish_to(
            routing_key = 'sms.outbound.%s' % self.config.get('TRANSPORT_NAME'),
            exchange_name = self.config.get('exchange_name'),
            exchange_type = self.config.get('exchange_type')
        )
        
        # consume everything that matches a wildcard
        self.consume(
            routing_key = "sms.inbound.%s.*" % self.config.get('TRANSPORT_NAME'), 
            callback = self.consume_message,
            exchange_name = self.config.get('exchange_name'),
            exchange_type = self.config.get('exchange_type')
        )
    
    def consume_message(self, message):
        dictionary = message.payload
        try:
            params = [
                ("callback_name", "sms_received"),
                ("to_msisdn", str(dictionary.get('destination_addr'))),
                ("from_msisdn", str(dictionary.get('source_addr'))),
                ("message", str(dictionary.get('short_message')))
            ]
            url, resp = utils.callback(self.POST_TO_URL, params)
            log.msg('RESP: %s' % repr(resp))
        except Exception, e:
            log.err(e)
        
