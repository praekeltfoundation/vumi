from twisted.python import log
from twisted.web import xmlrpc
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from vumi.webapp.api.models import SentSMS

from vumi.service import Worker, Consumer, Publisher

class OperaConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    queue_name = routing_key = "sms.outbound.opera"
    
    def __init__(self, publisher, config):
        self.publisher = publisher
        self.proxy = xmlrpc.Proxy(config.get('url'))
        self.default_values = {
            'Service': config.get('service'),
            'Password': config.get('password'), 
            'Channel': config.get('channel'),
        }
    
    def consume_json(self, dictionary):
        dictionary = self.default_values.copy()
        delivery = dictionary.get('deliver_at', datetime.utcnow())
        expiry = dictionary.get('expire_at', (delivery + timedelta(days=1)))
        
        log.msg("Consumed JSON %s" % dictionary)
        
        sent_sms = SentSMS.objects.get(pk=dictionary['id'])
        
        dictionary['Numbers'] = dictionary.get('to_msisdn')
        dictionary['SMSText'] = dictionary.get('message')
        dictionary['Delivery'] = delivery
        dictionary['Expiry'] = expiry
        dictionary['Priority'] = dictionary.get('priority', 'standard')
        dictionary['Receipt'] = dictionary.get('receipt', 'Y')
        
        proxy_response = self.proxy.EAPIGateway.SendSMS(dictionary)
        
        sent_sms.transport_msg_id = proxy_response.get('Identifier')
        sent_sms.save()
    

class OperaPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "sms.inbound.opera.fallback"
    durable = True
    auto_delete = False
    delivery_mode = 2 # save to disk
    
    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s" % dictionary)
        super(OperaPublisher, self).publish_json(dictionary, **kwargs)
    

class OperaTransport(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the OperaTransport config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.start_publisher(OperaPublisher)
        # when it's done, create the consumer and pass it the publisher
        self.consumer = yield self.start_consumer(OperaConsumer, self.publisher, self.config)
    
    def stopWorker(self):
        log.msg("Stopping the OperaTransport")
    


