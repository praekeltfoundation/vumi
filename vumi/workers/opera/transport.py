from twisted.python import log
from twisted.web import xmlrpc
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from datetime import datetime, timedelta
from vumi.webapp.api.models import SentSMS
from vumi.webapp.api.gateways.opera import utils
from vumi.service import Worker, Consumer, Publisher
import cgi, simplejson

class OperaReceiptResource(Resource):
    
    def __init__(self, publisher):
        self.publisher = publisher
        Resource.__init__(self)
    
    def render_POST(self, request):
        receipts = utils.parse_receipts_xml(request.content.read())
        success, fail = [], []
        for receipt in receipts:
            try:
                # internally we store MSISDNs without a leading plus, strip that
                # from the msisdn
                sms = SentSMS.objects.get(
                                        transport_name = "Opera",
                                        transport_msg_id=receipt.reference, 
                                        to_msisdn=receipt.msisdn.replace("+",""))
                sms.transport_status = receipt.status
                sms.delivery_timestamp = datetime.strptime(receipt.timestamp, 
                                                        utils.OPERA_TIMESTAMP_FORMAT)
                sms.save()
                
                # FIXME: this no longer works, would publish to vumi.webapp.sms.receipt
                # losing any transport info.
                # signals.sms_receipt.send(sender=SentSMS, instance=sms, 
                #                             pk=sms.pk, 
                #                             receipt=receipt._asdict())
                success.append(receipt)
            except SentSMS.DoesNotExist, error:
                log.err()
                fail.append(receipt)
                
        
        request.setResponseCode(201)
        request.setHeader('Content-Type', 'application/json; charset-utf-8')
        return simplejson.dumps({
            'success': map(lambda rcpt: rcpt._asdict(), success),
            'fail': map(lambda rcpt: rcpt._asdict(), fail)
        })

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
    
    @inlineCallbacks
    def consume_json(self, json):
        dictionary = self.default_values.copy()
        dictionary.update(json)
        
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
        
        proxy_response = yield self.proxy.callRemote('EAPIGateway.SendSMS', dictionary)
        
        sent_sms.transport_msg_id = proxy_response.get('Identifier')
        sent_sms.save()
        returnValue(sent_sms)
    

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
        
        # start receipt web resource
        self.receipt_resource = yield self.start_web_resource(
            OperaReceiptResource(self.publisher), 
            self.config['receipt_path'],
            self.config['receipt_port']
        )
    
    def stopWorker(self):
        log.msg("Stopping the OperaTransport")
    


