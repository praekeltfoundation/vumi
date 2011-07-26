# -*- coding: utf-8 -*-
# -*- test-case-name: vumi.workers.opera.test_opera -*-
from twisted.python import log
from twisted.web import xmlrpc, http
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from datetime import datetime, timedelta
from vumi.webapp.api.models import SentSMS, ReceivedSMS, Keyword
from vumi.webapp.api.gateways.opera import utils
from vumi.utils import safe_routing_key
from vumi.webapp.api import forms
from vumi.message import Message, JSONMessageEncoder
from vumi.service import Worker, Consumer, Publisher
import cgi, json, iso8601, base64, xmlrpclib

class OperaHealthResource(Resource):
    isLeaf = True
    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "OK"

class OperaReceiptResource(Resource):
    
    def __init__(self, publisher):
        self.publisher = publisher
        Resource.__init__(self)
    
    def render_POST(self, request):
        receipts = utils.parse_receipts_xml(request.content.read())
        data = []
        for receipt in receipts:
            log.msg('Received delivery receipt:',receipt)
            dictionary = {
                'transport_name': 'Opera',
                'transport_msg_id': receipt.reference,
                'transport_status': receipt.status,
                'transport_delivered_at': datetime.strptime(
                    receipt.timestamp, 
                    utils.OPERA_TIMESTAMP_FORMAT
                )
            }
            message = Message(**dictionary)
            self.publisher.publish_message(message, routing_key='sms.receipt.opera')
            data.append(dictionary)
        
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'application/json; charset-utf-8')
        return json.dumps(data, cls=JSONMessageEncoder)

class OperaReceiveResource(Resource):
    
    def __init__(self, publisher):
        self.publisher = publisher
        Resource.__init__(self)
    
    def render_POST(self, request):
        content = request.content.read()
        sms = utils.parse_post_event_xml(content)
        self.publisher.publish_message(Message(**{
            'to_msisdn': sms['Local'],
            'from_msisdn': sms['Remote'],
            'message': sms['Text'],
            'transport_name': 'Opera',
            'received_at': iso8601.parse_date(sms['ReceiveDate'])
        }), routing_key = 'sms.inbound.opera.%s' % safe_routing_key(sms['Local']))
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/xml; charset=utf8')
        return content
    

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
    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        dictionary = self.default_values.copy()
        payload = message.payload

        delivery = payload.get('deliver_at', datetime.utcnow())
        expiry = payload.get('expire_at', (delivery + timedelta(days=1)))
        
        # check for non-ascii chars
        message = payload.get('message')
        if any(ord(c) > 127 for c in message):
            message = xmlrpclib.Binary(message.encode('utf-8'))

        dictionary['Numbers'] = payload.get('to_msisdn') or payload.get('recipient')
        dictionary['SMSText'] = message 
        dictionary['Delivery'] = delivery
        dictionary['Expiry'] = expiry
        dictionary['Priority'] = payload.get('priority', 'standard')
        dictionary['Receipt'] = payload.get('receipt', 'Y')
        
        log.msg("Sending SMS via Opera: %s" % dictionary)

        proxy_response = yield self.proxy.callRemote('EAPIGateway.SendSMS',
                dictionary)
        log.msg("Proxy response: %s" % proxy_response)
        
        if 'id' in payload:
            sent_sms = SentSMS.objects.get(pk=payload['id'])
            sent_sms.transport_msg_id = proxy_response.get('Identifier')
            sent_sms.save()
        
        returnValue(proxy_response)
    

class OperaPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "sms.inbound.opera.fallback"
    durable = True
    auto_delete = False
    delivery_mode = 2 # save to disk
    
    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s" % message)
        super(OperaPublisher, self).publish_message(message, **kwargs)
    

class OperaReceiptTransport(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting the OperaReceiptTransport config: %s' % self.config)
        self.publisher = yield self.start_publisher(OperaPublisher)
        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (OperaReceiptResource(self.publisher), self.config['web_receipt_path']),
                (OperaReceiveResource(self.publisher), self.config['web_receive_path']),
                (OperaHealthResource(), 'health'),
            ],
            self.config['web_port']
        )
    def stopWorker(self):
        log.msg('Stopping the OperaReceiptTransport')

class OperaSMSTransport(Worker):
    
    # inlineCallbacks, TwistedMatrix's fancy way of allowing you to write
    # asynchronous code as if it was synchronous by the nifty use of
    # coroutines.
    # See: http://twistedmatrix.com/documents/10.0.0/api/twisted.internet.defer.html#inlineCallbacks
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the OperaSMSTransport config: %s" % self.config)
        # create the publisher
        self.publisher = yield self.start_publisher(OperaPublisher)
        # when it's done, create the consumer and pass it the publisher
        self.consumer = yield self.start_consumer(OperaConsumer, self.publisher, self.config)
    
    def stopWorker(self):
        log.msg("Stopping the OperaSMSTransport")
    


