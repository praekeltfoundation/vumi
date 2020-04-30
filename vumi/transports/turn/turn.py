# -*- test-case-name: vumi.transports.turn.tests.test_turn -*-
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


def iso8601(turn_timestamp):
    # TODO: this will be a unix timestamp
    if turn_timestamp:
        ts = datetime.strptime(turn_timestamp, '%Y.%m.%d %H:%M:%S')
        return ts.isoformat()
    else:
        return ''


def normalize_outbound_msisdn(msisdn):
    if msisdn.startswith('+'):
        return msisdn.replace('+', '')
    else:
        return msisdn

class ReceiveEventResource(Resource):
    isLeaf = True

    def __init__(self, config, publish_event_func, publish_message):
        self.config = config
        self.publish_event_func = publish_event_func
        self.publish_inbound_func = publish_message
        self.transport_name = 'turn'

    @inlineCallbacks
    def do_render(self, request):
        request.setResponseCode(http.CREATED)
        # request.setHeader('Content-Type', 'text/plain')
        try:
            # TODO: validate HMAC secret

            for message in request.args.get("messages", []):
                message_id = '%s.%s' % (self.transport_name, message["id"])
                content = ''
                if message['type'] == 'text':
                    content = message["text"]["body"]
                elif message['type'] == 'location':
                    loc = message["location"]
                    content = 'geo:{},{}'.format(loc['latitude'], loc['longitude'])

                yield self.publish_inbound_func(
                    transport_name=self.transport_name,
                    transport_type='sms',
                    message_id=message_id,
                    transport_metadata={
                        'original_message_id': message_id,
                        'timestamp': iso8601(message["timestamp"]),
                        'network_id': request.args['provider'][0],
                        'keyword': request.args['keyword'][0],
                        },
                    to_addr=normalize_msisdn(self.config['to_addr']),
                    from_addr=normalize_msisdn(message['from']),
                    content=content,
                    )
                log.msg("Inbound Enqueued.")

            for event in request.args.get("statuses", []):
                message_id = '%s.%s' % (self.transport_name, event['id'])

                if event["status"] == 'sent':
                    delivery_status = 'pending'
                elif event["status"] == 'failed':
                    delivery_status = 'failed'
                elif status == 'delivered':
                    delivery_status = 'delivered'
                else:
                    continue

                yield self.publish_event_func(
                    user_message_id=message_id,
                    delivery_status=delivery_status,
                    transport_metadata={
                        'delivery_status': event["status"],
                        'timestamp': iso8601(event['timestamp']),
                        },
                    to_addr=normalize_msisdn(event['recipient_id']),
                    )
        except KeyError, e:
            request.setResponseCode(http.BAD_REQUEST)
            msg = ("Need more request keys to complete this request. \n\n"
                   "Missing request key: %s" % (e,))
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


class HttpResponseHandler(Protocol):
    def __init__(self, deferred):
        self.deferred = deferred
        self.stringio = StringIO()

    def dataReceived(self, bytes):
        self.stringio.write(bytes)

    def connectionLost(self, reason):
        self.deferred.callback(self.stringio.getvalue())


class TurnTransport(Transport):

    agent_factory = None  # For swapping out the Agent we use in tests.

    def mkres(self, cls):
        resource = cls(self.config, self.publish_delivery_report, self.publish_message)
        self._resources.append(resource)
        return (resource, self.config['web_%s_path' % (path_key,)])

    @inlineCallbacks
    def setup_transport(self):
        self._resources = []
        resources = [self.mkres(ReceiveEventResource)]
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
        handle messages arriving over AMQP meant for delivery via turn
        """

        message_params = {
            "preview_url": False,
            "recipient_type": "individual",
            "to": normalize_outbound_msisdn(message['to_addr']),
            "type": "text",
            "text": {"body": message['content']}
        }

        log.msg('Hitting %s with %s' % (self.config['url'], message_params))
        log.msg(urlencode(message_params))

        try:
            response = yield http_request_full(
                self.config['url'], message_params.to_json(), {
                    'User-Agent': ['Vumi Turn Transport'],
                    'Content-Type': ['application/json'],
                    'Authorization': ['Bearer {}'.format(TURN_TOKEN)],
                    }, 'POST', agent_class=self.agent_factory)
        except ConnectionRefusedError:
            log.msg("Connection failed sending message:", message)
            raise TemporaryFailure('connection refused')

        log.msg('Headers', list(response.headers.getAllRawHeaders()))

        if response.code != 200:
            raise PermanentFailure('server error: HTTP %s: %s'
                                   % (response.code, response.delivered_body))

        transport_message_id = json.loads(response.content)["messages"][0]["id"]
        yield self.publish_ack(
            user_message_id=message['message_id'],
            sent_message_id=transport_message_id,
            )

    def stopWorker(self):
        """shutdown"""
        super(TurnTransport, self).stopWorker()
        if hasattr(self, 'receipt_resource'):
            return self.receipt_resource.stopListening()
