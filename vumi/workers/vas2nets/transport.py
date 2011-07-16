# -*- test-case-name: vumi.workers.vas2nets.test_vas2nets -*-

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Protocol
from twisted.internet import reactor

from StringIO import StringIO
from vumi.utils import StringProducer
from vumi.message import Message
from vumi.service import Worker
from vumi.errors import VumiError

from urllib import urlencode
from datetime import datetime

def iso8601(vas2nets_timestamp):
    ts = datetime.strptime(vas2nets_timestamp, '%Y.%m.%d %H:%M:%S')
    return ts.isoformat()

class Vas2NetsTransportError(VumiError): pass

class ReceiveSMSResource(Resource):
    isLeaf = True
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher
    
    def render(self, request):
        
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        try:
            with self.publisher.transaction():
                self.publisher.publish_message(Message(**{
                    'transport_message_id': request.args['messageid'][0],
                    'transport_timestamp': iso8601(request.args['time'][0]),
                    'transport_network_id': request.args['provider'][0],
                    'transport_keyword': request.args['keyword'][0],
                    'to_msisdn': request.args['destination'][0],
                    'from_msisdn': request.args['sender'][0],
                    'message': request.args['text'][0]
                }), routing_key='sms.inbound.%s.%s' % (
                    self.config.get('transport_name'), 
                    request.args['destination'][0]
                ))
            return ''
        except KeyError, e:
            request.setResponseCode(http.BAD_REQUEST)
            return "Need more request keys to complete this request. \n\n" \
                    "Missing request key: %s" % e

class DeliveryReceiptResource(Resource):
    isLeaf = True
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher
    
    def render_POST(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        with self.publisher.transaction():
            self.publisher.publish_message(Message(**{
                'transport_message_id': request.args['smsid'][0],
                'transport_status': request.args['status'][0],
                'transport_status_message': request.args['text'][0],
                'transport_timestamp': iso8601(request.args['time'][0]),
                'transport_network_id': request.args['provider'][0],
                'to_msisdn': request.args['sender'][0],
                'id': request.args['messageid'][0]
            }), routing_key='sms.receipt.%(transport_name)s' % self.config)
            return ''

class HealthResource(Resource):
    isLeaf = True
    def __init__(self, publisher):
        self.publisher
    
    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "OK"


class HttpResponseReader(Protocol):
    def __init__(self, deferrred):
        self.deferred = deferred
        self.stringio = StringIO()

    def dataReceived(self, bytes):
        self.stringio.write(bytes)

    def connectionLost(self, reason):
        self.finished.callback(self.stringio.buf)


class Vas2NetsTransport(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        """called by the Worker class when the AMQP connections been established"""
        self.publisher = yield self.publish_to('sms.inbound.%(transport_name)s.fallback' % self.config)
        self.consumer = yield self.consume('sms.outbound.%(transport_name)s' % self.config, 
                                    self.handle_outbound_message)
        self.receipt_resource = yield self.start_web_resources(
            [
                (ReceiveSMSResource(self.config, self.publisher), self.config['web_receive_path']),
                (DeliveryReceiptResource(self.config, self.publisher), self.config['web_receipt_path']),
                (HealthResource(), 'health'),
            ],
            self.config['web_port']
        )
        
    
    def handle_outbound_message(self, message):
        """handle messages arriving over AMQP meant for delivery via vas2nets"""
        data = message.payload
        request_params = {
            'username': self.config['username'],
            'password': self.config['password'],
            'call-number': data['to_msisdn'],
            'origin': data['from_msisdn'],
            'messageid': data.get('reply_to') or data['id'],
            'provider': data['provider'],
            'tariff': data.get('tariff', 0),
            'text': data['message'],
            'owner': self.config['owner'],
            'service': self.config['service'],
            'subservice': self.config['subservice'],
        }
        
        agent = Agent(reactor)
        deferred = agent.request('POST', self.config['url'], 
            Headers({'User-Agent': ['Vumi Vas2Net Transport']}),
            StringProducer(urlencode(request_params))
        )
        
        @inlineCallbacks
        def _cb_success(self, response):
            deferred = Deferred()
            response.deliverBody(HttpResponseHandler(deferred))
            response_content = yield deferred
            
            if response.headers.hasHeader('X-VAS2Nets-SmsId'):
                transport_message_id = response.headers.getRawHeaders('X-VAS2Nets-SmsId')[0]
                with self.publisher.transaction():
                    self.publisher.publish_message(Message(**{
                        'id': data['id'],
                        'transport_message_id': transport_message_id
                    }), routing_key='sms.%(transport_name)s.ack')
            else:
                raise Vas2NetsTransportError('No SmsId Header, content: %s' % 
                                                response_content)
        
        deferred.addCallback(_cb_success)
        return deferred
        
    def stopWorker(self):
        """shutdown"""
        pass
    