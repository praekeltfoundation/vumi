# -*- test-case-name: vumi.workers.vas2nets.test_vas2nets -*-
# -*- encoding: utf-8 -*-

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.python import log
from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet.protocol import Protocol
from twisted.internet import reactor

from StringIO import StringIO
from vumi.utils import StringProducer, normalize_msisdn
from vumi.message import Message
from vumi.service import Worker
from vumi.errors import VumiError

from urllib import urlencode
from datetime import datetime
import string
import warnings

def iso8601(vas2nets_timestamp):
    if vas2nets_timestamp:
        ts = datetime.strptime(vas2nets_timestamp, '%Y.%m.%d %H:%M:%S')
        return ts.isoformat()
    else:
        return ''

def validate_characters(chars):
    single_byte_set = ''.join([
        string.ascii_lowercase,     # a-z
        string.ascii_uppercase,     # A-Z
        u'0123456789',
        u'äöüÄÖÜàùòìèé§Ññ£$@',
        u' ',
        u'/?!#%&()*+,-:;<=>."\'',
        u'\n\r',
    ])
    double_byte_set = u'|{}[]€\~^'
    superset = single_byte_set + double_byte_set
    for char in chars:
        if char not in superset:
            raise Vas2NetsEncodingError, 'illegal character %s' % char
        if char in double_byte_set:
            warnings.warn(''.join['double byte character %s, max SMS length',
                                    ' is 70 chars as a result'] % char, 
                                Vas2NetsEncodingWarning)
    return chars

def normalize_outbound_msisdn(msisdn):
    if msisdn.startswith('+'):
        return msisdn.replace('+','00')
    else:
        return msisdn


class Vas2NetsTransportError(VumiError): pass
class Vas2NetsEncodingError(VumiError): pass
class Vas2NetsEncodingWarning(VumiError): pass

class ReceiveSMSResource(Resource):
    isLeaf = True
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher

    @inlineCallbacks
    def do_render(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        try:
            yield self.publisher.publish_message(Message(**{
                'transport_message_id': request.args['messageid'][0],
                'transport_timestamp': iso8601(request.args['time'][0]),
                'transport_network_id': request.args['provider'][0],
                'transport_keyword': request.args['keyword'][0],
                'to_msisdn': normalize_msisdn(request.args['destination'][0]),
                'from_msisdn': normalize_msisdn(request.args['sender'][0]),
                'message': request.args['text'][0]
            }), routing_key='sms.inbound.%s.%s' % (
                self.config.get('transport_name'),
                request.args['destination'][0]
            ))
            log.msg("Enqueued.")
        except KeyError, e:
            request.setResponseCode(http.BAD_REQUEST)
            request.write("Need more request keys to complete this request. \n\n" \
                              "Missing request key: %s" % e)
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET

class DeliveryReceiptResource(Resource):
    isLeaf = True
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher
    
    def render_POST(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        # with self.publisher.transaction():
        if 1:
            self.publisher.publish_message(Message(**{
                'transport_message_id': request.args['smsid'][0],
                'transport_status': request.args['status'][0],
                'transport_status_message': request.args['text'][0],
                'transport_timestamp': iso8601(request.args['time'][0]),
                'transport_network_id': request.args['provider'][0],
                'to_msisdn': normalize_msisdn(request.args['sender'][0]),
                'id': request.args['messageid'][0]
            }), routing_key='sms.receipt.%(transport_name)s' % self.config)
            return ''

class HealthResource(Resource):
    isLeaf = True
    
    def render(self, request):
        request.setResponseCode(http.OK)
        return 'OK'

class HttpResponseHandler(Protocol):
    def __init__(self, deferred):
        self.deferred = deferred
        self.stringio = StringIO()

    def dataReceived(self, bytes):
        self.stringio.write(bytes)

    def connectionLost(self, reason):
        self.deferred.callback(self.stringio.getvalue())


class Vas2NetsTransport(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        """called by the Worker class when the AMQP connections been established"""
        self.publisher = yield self.publish_to('sms.inbound.%(transport_name)s.fallback' % self.config)
        self.consumer = yield self.consume('sms.outbound.%(transport_name)s' % self.config, 
                                    self.handle_outbound_message)
        # don't care about prefetch window size
        # but only want one message sent to me at a time, this'll 
        # throttle our output to 1 msg at a time, which means 
        # 1 transport = 1 connection, 10 transports is max 10 connections at a time.
        # and make it apply only to this channel
        self.consumer.channel.basic_qos(0,int(self.config.get('throttle', 1)), False)

        self.receipt_resource = yield self.start_web_resources(
            [
                (ReceiveSMSResource(self.config, self.publisher), self.config['web_receive_path']),
                (DeliveryReceiptResource(self.config, self.publisher), self.config['web_receipt_path']),
                (HealthResource(), 'health'),
            ],
            self.config['web_port']
        )
        
    
    @inlineCallbacks
    def handle_outbound_message(self, message):
        """handle messages arriving over AMQP meant for delivery via vas2nets"""
        data = message.payload
        
        default_params = {
            'username': self.config['username'],
            'password': self.config['password'],
            'owner': self.config['owner'],
            'service': self.config['service'],
            'subservice': self.config['subservice'],
        }
        
        request_params = {
            'call-number': normalize_outbound_msisdn(data['to_msisdn']),
            'origin': data['from_msisdn'],
            'messageid': data.get('reply_to', data['id']),
            'provider': data['transport_network_id'],
            'tariff': data.get('tariff', 0),
            'text': validate_characters(data['message']),
            'subservice': data.get('transport_keyword',
                            self.config['subservice'])
        }
        
        default_params.update(request_params)
        
        log.msg('Hitting %s with %s' % (self.config['url'], default_params))
        log.msg(urlencode(default_params))
        
        agent = Agent(reactor)
        response = yield agent.request('POST', self.config['url'], 
            Headers({
                'User-Agent': ['Vumi Vas2Net Transport'],
                'Content-Type': ['application/x-www-form-urlencoded'],
            }),
            StringProducer(urlencode(default_params))
        )
        
        deferred = Deferred()
        response.deliverBody(HttpResponseHandler(deferred))
        response_content = yield deferred
        
        log.msg('Headers', list(response.headers.getAllRawHeaders()))
        header = self.config.get('header', 'X-Nth-Smsid')
        
        if response.headers.hasHeader(header):
            transport_message_id = response.headers.getRawHeaders(header)[0]
            # with self.publisher.transaction():
            if 1:
                self.publisher.publish_message(Message(**{
                    'id': data['id'],
                    'transport_message_id': transport_message_id
                }), routing_key='sms.ack.%(transport_name)s' % self.config)
        else:
            raise Vas2NetsTransportError('No SmsId Header, content: %s' % 
                                            response_content)
        
    def stopWorker(self):
        """shutdown"""
        pass
    
