# -*- test-case-name: vumi.transports.opera.tests.test_opera -*-
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import xmlrpclib
import json
import redis

import iso8601
from twisted.python import log
from twisted.web import xmlrpc, http
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.webapp.api.gateways.opera import utils
from vumi.utils import safe_routing_key, normalize_msisdn, get_deploy_int
from vumi.message import Message, JSONMessageEncoder
from vumi.service import Worker, Consumer, Publisher
from vumi.transports import Transport


class OperaHealthResource(Resource):
    isLeaf = True

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "OK"


class OperaReceiptResource(Resource):

    def __init__(self, callback):
        self.callback = callback
        Resource.__init__(self)

    def render_POST(self, request):
        receipts = utils.parse_receipts_xml(request.content.read())
        for receipt in receipts:
            self.callback(receipt)

        request.setResponseCode(http.OK)
        return ''


class OperaReceiveResource(Resource):

    def __init__(self, callback):
        self.callback = callback
        Resource.__init__(self)

    def render_POST(self, request):
        content = request.content.read()
        sms = utils.parse_post_event_xml(content)
        self.callback(
            to_addr=normalize_msisdn(sms['Local'], country_code='27'), 
            from_addr=normalize_msisdn(sms['Remote'], country_code='27'), 
            content=sms['Text'], transport_type='sms', 
            message_id=sms['MessageID'], transport_metadata={
                'provider': sms['MobileNetwork']
            })
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
        self.opera_url = self.config['url']
        self.transport_name = self.config['transport_name']
        self.redis_config = self.config.get('redis', {})

    def set_message_id_for_identifier(self, identifier, message_id):
        rkey = '%s#%s' % (self.r_prefix, identifier)
        self.r_server.set(rkey, message_id)
        self.r_server.expire(rkey, self.MESSAGE_ID_LIFETIME)
    
    def get_message_id_for_identifier(self, identifier):
        rkey = '%s#%s' % (self.r_prefix, identifier)
        return self.r_server.get(rkey)
    
    def handle_raw_incoming_receipt(self, receipt):
        # convert delivery receipt status values
        status_map = {
            'D': 'delivered',
            'd': 'delivered',
            'R': 'delivered',
            'Q': 'pending',
            'P': 'pending',
            'B': 'pending',
            'a': 'pending',
            'u': 'pending'
        }
        
        internal_status = status_map.get(receipt.status, 'failed')
        internal_message_id = self.get_message_id_for_identifier(receipt.reference)
        self.publish_delivery_report(internal_message_id, internal_status)

    
    @inlineCallbacks
    def setup_transport(self):
        log.msg('Starting the OperaInboundTransport config: %s' % self.transport_name)
        dbindex = get_deploy_int(self._amqp_client.vhost)
        self.r_server = redis.Redis(db=dbindex, **self.redis_config)
        self.r_prefix = "%(transport_name)s@%(url)s" % self.config
        # start receipt web resource
        self.web_resource = yield self.start_web_resources(
            [
                (OperaReceiptResource(self.handle_raw_incoming_receipt),
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

    MESSAGE_ID_LIFETIME = 60 * 60 * 48 # 48 hours

    def validate_config(self):
        self.opera_url = self.config['url']
        self.opera_channel = self.config['channel']
        self.opera_password = self.config['password']
        self.opera_service = self.config['service']
        self.transport_name = self.config['transport_name']
        self.redis_config = self.config.get('redis', {})

    
    def setup_transport(self):
        log.msg("Starting the OperaOutboundTransport: %s" % self.transport_name)
        self.proxy = xmlrpc.Proxy(self.opera_url)
        dbindex = get_deploy_int(self._amqp_client.vhost)
        self.r_server = redis.Redis(db=dbindex, **self.redis_config)
        self.r_prefix = "%(transport_name)s@%(url)s" % self.config
        self.default_values = {
            'Service': self.opera_service,
            'Password': self.opera_password,
            'Channel': self.opera_channel,
        }
    
    def set_message_id_for_identifier(self, identifier, message_id):
        rkey = '%s#%s' % (self.r_prefix, identifier)
        self.r_server.set(rkey, message_id)
        self.r_server.expire(rkey, self.MESSAGE_ID_LIFETIME)
    
    def get_message_id_for_identifier(self, identifier):
        rkey = '%s#%s' % (self.r_prefix, identifier)
        return self.r_server.get(rkey)

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
        transport_message_id = proxy_response['Identifier']

        self.set_message_id_for_identifier(transport_message_id, 
            message['message_id'])

        yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=transport_message_id)

    def teardown_transport(self):
        log.msg("Stopping the OperaOutboundTransport: %s" % self.transport_name)
