# -*- test-case-name: vumi.transports.opera.tests.test_opera -*-
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import xmlrpclib
import json

import iso8601
from twisted.python import log
from twisted.web import xmlrpc, http
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.webapp.api.gateways.opera import utils
from vumi.utils import safe_routing_key
from vumi.message import Message, JSONMessageEncoder
from vumi.service import Worker, Consumer, Publisher
from vumi.transports import Transport


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
            log.msg('Received delivery receipt:', receipt)
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
            self.publisher.publish_message(message,
                                           routing_key='sms.receipt.opera')
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
        }), routing_key='sms.inbound.opera.%s' % safe_routing_key(
                sms['Local']))
        request.setResponseCode(http.OK)
        request.setHeader('Content-Type', 'text/xml; charset=utf8')
        return content



class OperaInboundTransport(Transport):

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        self.web_receipt_path = self.config['web_receipt_path']
        self.web_receive_path = self.config['web_receive_path']
        self.web_port = int(self.config['web_port'])

    @inlineCallbacks
    def setup_transport(self):
        log.msg('Starting the OperaInboundTransport config: %s' % self.transport_name)
        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (OperaReceiptResource(self.publish_message),
                 self.web_receipt_path),
                (OperaReceiveResource(self.publish_message),
                 self.web_receive_path),
                (OperaHealthResource(), 'health'),
            ],
            self.web_port
        )

    @inlineCallbacks
    def teardown_transport(self):
        yield self.web_resource.loseConnection()



class OperaOutboundTransport(Transport):
    """
    This is a separate transport from the OperaInboundTransport because after
    having run this in production for a while it turned out that Opera was 
    quite slow and we needed to have concurrent outbound connections so we
    could clear our queues in parallel. Having the inbound & outbound transports
    be the same logic created problems as the inbound would try to create
    multiple HTTP Resources which we didn't need.
    """

    def validate_config(self):
        self.opera_url = self.config['url']
        self.opera_channel = self.config['channel']
        self.opera_password = self.config['password']
        self.opera_service = self.config['service']
    
    def setup_transport(self):
        log.msg("Starting the OperaOutboundTransport: %s" % self.transport_name)
        self.proxy = xmlrpc.Proxy(self.opera_url)
        self.default_values = {
            'Service': self.opera_service,
            'Password': self.opera_password,
            'Channel': self.opera_channel,
        }
    
    @inlineCallbacks
    def handle_outbound_message(self, message):
        xmlrpc_payload = self.default_values.copy()
        metadata = message["transport_metadata"]

        delivery = metadata.get('deliver_at', datetime.utcnow())
        expiry = metadata.get('expire_at', (delivery + timedelta(days=1)))
        priority = metadata.get('priority', 'standard')
        receipt = metadata.get('receipt', 'Y')

        # check for non-ascii chars
        content = message["content"]
        if any(ord(c) > 127 for c in content):
            content = xmlrpclib.Binary(content.encode('utf-8'))

        xmlrpc_payload['Numbers'] = message['to_addr']
        xmlrpc_payload['SMSText'] = content
        xmlrpc_payload['Delivery'] = delivery
        xmlrpc_payload['Expiry'] = expiry
        xmlrpc_payload['Priority'] = priority
        xmlrpc_payload['Receipt'] = receipt

        log.msg("Sending SMS via Opera: %s" % xmlrpc_payload)

        proxy_response = yield self.proxy.callRemote('EAPIGateway.SendSMS',
            xmlrpc_payload)

        log.msg("Proxy response: %s" % proxy_response)

        yield self.publish_ack(
            user_message_id=message['message_id'],
            sent_message_id=proxy_response.get('Identifier'))

    def teardown_transport(self):
        log.msg("Stopping the OperaOutboundTransport: %s" % self.transport_name)
