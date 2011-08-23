from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource

from vumi.utils import http_request, safe_routing_key
from vumi.message import Message
from vumi.service import Worker
from vumi.workers.integrat.utils import HigateXMLParser

hxg = HigateXMLParser()


class IntegratHttpResource(Resource):
    def __init__(self, config, publisher):
        self.config = config
        self.publisher = publisher

    def render(self, request):
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/plain')
        hxg_msg = hxg.parse(request.content.read())
        text = hxg_msg.get('USSText', '').strip()
        if hxg_msg['EventType'] == 'Request':
            if text == 'REQ':
                event_type = 'new_session'
            else:
                event_type = 'resume_session'
        else:
            event_type = '%s_session' % hxg_msg['EventType'].lower()

        routing_key = 'ussd.inbound.%s.%s' % (
            self.config['transport_name'],
            safe_routing_key(hxg_msg['ConnStr'])
        )
        msg = Message(**{
            'transport_session_id': hxg_msg['SessionID'],
            'transport_message_type': event_type,
            'message': text,
            'recipient': hxg_msg['ConnStr'],
            'sender': hxg_msg['MSISDN'],
        })
        log.msg('publishing', msg, routing_key)
        self.publisher.publish_message(msg, routing_key=routing_key)
        return ''


class HealthResource(Resource):
    isLeaf = True

    def render(self, request):
        request.setResponseCode(http.OK)
        return 'OK'


class IntegratOutboundTransport(Worker):
    @inlineCallbacks
    def startWorker(self):
        self.consumer = yield self.consume(
            'ussd.outbound.%(transport_name)s' % self.config,
            self.handle_outbound_message)

    @inlineCallbacks
    def handle_outbound_message(self, message):
        data = message.payload
        yield http_request(self.config['url'], hxg.build({
            'Flags': data.get('close', '0'),
            'SessionID': data['transport_session_id'],
            'Type': 'USSReply',
            'USSText': data['message'],
            'Password': self.config.get('password'),
            'UserID': self.config.get('username')
        }), headers={
            'Content-Type': ['text/xml; charset=utf-8']
        })


class IntegratInboundTransport(Worker):

    @inlineCallbacks
    def startWorker(self):
        """
        called by the Worker class when the AMQP connections been established
        """
        self.publisher = yield self.publish_to(
            'ussd.inbound.%(transport_name)s.fallback' % self.config)
        self.web_resource = yield self.start_web_resources(
            [
                (IntegratHttpResource(self.config, self.publisher),
                 self.config['web_path']),
                (HealthResource(), 'health'),
            ],
            self.config['web_port']
        )
