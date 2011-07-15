# -*- test-case-name: vumi.workers.vas2nets.test_vas2nets -*-

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET

from vumi.message import Message
from vumi.service import Worker

class ReceiveSMSResource(Resource):
    isLeaf = True
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher
    
    def render_POST(self, request):
        
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        
        self.publisher.publish_message(Message(**{
            'transport_message_id': request.args['messageid'],
            'transport_timestamp': request.args['time'],
            'to_msisdn': request.args['destination'],
            'from_msisdn': request.args['sender'],
            'message': request.args['text']
        }), routing_key='sms.inbound.%s.%s' % (
            self.config.get('transport_name'), 
            request.args['destination']
        ))
        return ''
    

class DeliveryReceiptResource(Resource):
    isLeaf = True
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher
    
    def render_POST(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        self.publisher.publish_message(Message(**{
            'transport_message_id': request.args['smsid'],
            'transport_status': request.args['status'],
            'transport_status_message': request.args['text'],
            'transport_timestamp': request.args['time'],
        }), routing_key='sms.receipt.%(transport_name)s' % self.config)
        return ''

class Vas2NetsTransport(Worker):
    
    def startWorker(self):
        """called by the Worker class when the AMQP connections been established"""
        pass
    
    def stopWorker(self):
        """shutdown"""
        pass
    