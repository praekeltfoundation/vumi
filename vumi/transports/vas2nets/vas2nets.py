# -*- test-case-name: vumi.transports.vas2nets.tests.test_vas2nets -*-
# -*- encoding: utf-8 -*-

from urllib import urlencode
from datetime import datetime
import string
import warnings
from StringIO import StringIO

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.protocol import Protocol
from twisted.internet.error import ConnectionRefusedError

from vumi.utils import http_request_full, normalize_msisdn, LogFilterSite
from vumi.transports.base import Transport
from vumi.transports.failures import TemporaryFailure, PermanentFailure
from vumi.errors import VumiError


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
            raise Vas2NetsEncodingError('illegal character %s' % char)
        if char in double_byte_set:
            warnings.warn(''.join['double byte character %s, max SMS length',
                                  ' is 70 chars as a result'] % char,
                          Vas2NetsEncodingWarning)
    return chars


def normalize_outbound_msisdn(msisdn):
    if msisdn.startswith('+'):
        return msisdn.replace('+', '00')
    else:
        return msisdn


class Vas2NetsTransportError(VumiError):
    pass


class Vas2NetsEncodingError(VumiError):
    pass


class Vas2NetsEncodingWarning(VumiError):
    pass


class ReceiveSMSResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    @inlineCallbacks
    def do_render(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        try:
            message_id = '%s.%s' % (self.transport_name,
                                    request.args['messageid'][0])
            yield self.publish_func(
                transport_name=self.transport_name,
                transport_type='sms',
                message_id=message_id,
                transport_metadata={
                    'original_message_id': message_id,
                    'timestamp': iso8601(request.args['time'][0]),
                    'network_id': request.args['provider'][0],
                    'keyword': request.args['keyword'][0],
                    },
                to_addr=normalize_msisdn(request.args['destination'][0]),
                from_addr=normalize_msisdn(request.args['sender'][0]),
                content=request.args['text'][0],
                )
            log.msg("Enqueued.")
        except KeyError, e:
            request.setResponseCode(http.BAD_REQUEST)
            msg = "Need more request keys to complete this request. \n\n" \
                    "Missing request key: %s" % e
            log.msg('Returning %s: %s' % (http.BAD_REQUEST, msg))
            request.write(msg)
        except ValueError, e:
            request.setResponseCode(http.BAD_REQUEST)
            msg = "ValueError: %s" % e
            log.msg('Returning %s: %s' % (http.BAD_REQUEST, msg))
            request.write(msg)
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.err("Error processing request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class DeliveryReceiptResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        self.config = config
        self.publish_func = publish_func
        self.transport_name = self.config['transport_name']

    @inlineCallbacks
    def do_render(self, request):
        log.msg('got hit with %s' % request.args)
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        try:
            message_id = '%s.%s' % (self.transport_name,
                                    request.args['messageid'][0])
            status = int(request.args['status'][0])
            delivery_status = 'pending'
            if status < 0:
                delivery_status = 'failed'
            elif status in [2, 14]:
                delivery_status = 'delivered'
            yield self.publish_func(
                user_message_id=message_id,
                delivery_status=delivery_status,
                transport_metadata={
                    'delivery_status': request.args['status'][0],
                    'delivery_message': request.args['text'][0],
                    'timestamp': iso8601(request.args['time'][0]),
                    'network_id': request.args['provider'][0],
                    },
                to_addr=normalize_msisdn(request.args['sender'][0]),
                )
        except KeyError, e:
            request.setResponseCode(http.BAD_REQUEST)
            msg = "Need more request keys to complete this request. \n\n" \
                    "Missing request key: %s" % e
            log.msg('Returning %s: %s' % (http.BAD_REQUEST, msg))
            request.write(msg)
        except ValueError, e:
            request.setResponseCode(http.BAD_REQUEST)
            msg = "ValueError: %s" % e
            log.msg('Returning %s: %s' % (http.BAD_REQUEST, msg))
            request.write(msg)
        except Exception, e:
            request.setResponseCode(http.INTERNAL_SERVER_ERROR)
            log.err("Error processing request: %s" % (request,))
        request.finish()

    def render(self, request):
        self.do_render(request)
        return NOT_DONE_YET


class HealthResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_func):
        pass

    def render(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return 'OK'


class HttpResponseHandler(Protocol):
    def __init__(self, deferred):
        self.deferred = deferred
        self.stringio = StringIO()

    def dataReceived(self, bytes):
        self.stringio.write(bytes)

    def connectionLost(self, reason):
        self.deferred.callback(self.stringio.getvalue())


class Vas2NetsTransport(Transport):
    def mkres(self, cls, publish_func, path_key):
        resource = cls(self.config, publish_func)
        self._resources.append(resource)
        return (resource, self.config['web_%s_path' % (path_key,)])

    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        self.config.setdefault('web_health_path', 'health')
        resources = [
            self.mkres(ReceiveSMSResource, self.publish_message, 'receive'),
            self.mkres(DeliveryReceiptResource, self.publish_delivery_report,
                       'receipt'),
            self.mkres(HealthResource, None, 'health'),
            ]
        self.receipt_resource = yield self.start_web_resources(
            resources, self.config['web_port'], LogFilterSite)

    def get_transport_url(self):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used in
        non-test code, because the API might live behind a load balancer or
        proxy.
        """
        addr = self.receipt_resource.getHost()
        return "http://%s:%s" % (addr.host, addr.port)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        """
        handle messages arriving over AMQP meant for delivery via vas2nets
        """

        params = {
            'username': self.config['username'],
            'password': self.config['password'],
            'owner': self.config['owner'],
            'service': self.config['service'],
        }

        v2n_message_id = message.get('in_reply_to')
        if v2n_message_id is not None:
            if v2n_message_id.startswith(self.transport_name):
                v2n_message_id = v2n_message_id[len(self.transport_name) + 1:]
        else:
            v2n_message_id = message['message_id']

        message_params = {
            'call-number': normalize_outbound_msisdn(message['to_addr']),
            'origin': message['from_addr'],
            'messageid': v2n_message_id,
            'provider': message['transport_metadata']['network_id'],
            'tariff': message['transport_metadata'].get('tariff', 0),
            'text': validate_characters(message['content']),
            'subservice': self.config.get('subservice',
                                          message['transport_metadata'].get(
                                              'keyword', '')),
        }

        params.update(message_params)

        log.msg('Hitting %s with %s' % (self.config['url'], params))
        log.msg(urlencode(params))

        try:
            response = yield http_request_full(
                self.config['url'], urlencode(params), {
                    'User-Agent': ['Vumi Vas2Net Transport'],
                    'Content-Type': ['application/x-www-form-urlencoded'],
                    }, 'POST')
        except ConnectionRefusedError:
            log.msg("Connection failed sending message:", message)
            raise TemporaryFailure('connection refused')

        log.msg('Headers', list(response.headers.getAllRawHeaders()))
        header = self.config.get('header', 'X-Nth-Smsid')

        if response.code != 200:
            raise PermanentFailure('server error: HTTP %s: %s'
                                   % (response.code, response.delivered_body))

        if response.headers.hasHeader(header):
            transport_message_id = response.headers.getRawHeaders(header)[0]
            yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=transport_message_id,
                )
        else:
            err_msg = 'No SmsId Header, content: %s' % response.delivered_body
            yield self.publish_nack(user_message_id=message['message_id'],
                sent_message_id=message['message_id'],
                reason=err_msg)
            raise Vas2NetsTransportError(err_msg)

    def stopWorker(self):
        """shutdown"""
        super(Vas2NetsTransport, self).stopWorker()
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()
