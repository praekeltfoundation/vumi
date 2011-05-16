from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from django.contrib.auth.models import User
from vumi.service import Worker
from vumi.message import Message
from vumi.webapp.api import utils
from vumi.webapp.api.models import SentSMS, SentSMSBatch

class TopicSmppWorker(Worker):
    
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
        to_msisdn = dictionary.get('destination_addr')
        from_msisdn = dictionary.get('source_addr')
        message = dictionary.get('short_message')
        log.msg('dictionary', dictionary)
        try:
            params = [
                ("callback_name", "sms_received"),
                ("to_msisdn", str(to_msisdn)),
                ("from_msisdn", str(from_msisdn)),
                ("message", str(message))
            ]
            url, resp = utils.callback(self.config.get('POST_TO_URL'), params)
            log.msg('RESP: %s' % repr(resp))
            
            # create a new message to be sent out, it needs to be linked
            # to a User for accounting purposes
            user, created = User.objects.get_or_create(username=self.config.get('USER_ACCOUNT'))
            batch = SentSMSBatch.objects.create(title='', user=user)
            sms = batch.sentsms_set.create(user=user, 
                to_msisdn=from_msisdn, 
                from_msisdn=to_msisdn,
                transport_name=self.config.get('TRANSPORT_NAME'),
                message=resp
            )
            
            self.publisher.publish_message(
                Message(
                    id=sms.pk, 
                    to_msisdn=sms.to_msisdn, 
                    message=sms.message, 
                    from_msisdn=sms.from_msisdn
                ),
                routing_key='sms.outbound.%s.%s' % (
                    self.config.get('TRANSPORT_NAME'), sms.to_msisdn
                )
            )
            
        except Exception, e:
            log.err(e)
        
