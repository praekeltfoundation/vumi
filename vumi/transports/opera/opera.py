# -*- test-case-name: vumi.transports.opera.tests.test_opera -*-
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import redis

from twisted.python import log
from twisted.web import xmlrpc, http
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks

from vumi.utils import normalize_msisdn, get_deploy_int
from vumi.transports import Transport
from vumi.transports.failures import TemporaryFailure, PermanentFailure
from vumi.transports.opera import utils


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


class OperaTransport(Transport):
    """
    Base class that provides Redis mechanics for maintaining mapping
    of internal vs external message ids, used to link a delivery
    receipt back to the message of the message that was originally
    sent out.
    """

    # After how many seconds should the transport expire keys
    # and disregard delivery reports? Defaults to a week.
    DEFAULT_MESSAGE_ID_LIFETIME = 60 * 60 * 24 * 7

    def validate_config(self):
        """
        Transport-specific config validation happens in here.
        """
        self.message_id_lifetime = self.config.get('message_id_lifetime',
                                           self.DEFAULT_MESSAGE_ID_LIFETIME)
        self.web_receipt_path = self.config['web_receipt_path']
        self.web_receive_path = self.config['web_receive_path']
        self.web_port = int(self.config['web_port'])
        self.opera_url = self.config['url']
        self.opera_channel = self.config['channel']
        self.opera_password = self.config['password']
        self.opera_service = self.config['service']
        self.transport_name = self.config['transport_name']

    def set_message_id_for_identifier(self, identifier, message_id):
        """
        Link an external message id, the identifier, to an internal
        message id for `MAX_ID_LIFETIME` amount of seconds

        :type identifier: str
        :param identifier:
            The message id we get back from Opera
        :type message_id: str
        :param message_id:
            The internal message id that was used when the message was sent.
        """
        rkey = '%s#%s' % (self.r_prefix, identifier)
        self.r_server.set(rkey, message_id)
        self.r_server.expire(rkey, self.message_id_lifetime)

    def get_message_id_for_identifier(self, identifier):
        """
        Get an internal message id for a given identifier

        :type identifier: str
        :param identifier:
            The message id we originally got from Opera when the message
            was accepted for delivery.

        """
        rkey = '%s#%s' % (self.r_prefix, identifier)
        return self.r_server.get(rkey)

    def handle_raw_incoming_receipt(self, receipt):
        # convert delivery receipt status values, anything not in
        # this status map defaults to `failed`
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
        internal_message_id = self.get_message_id_for_identifier(
            receipt.reference)
        self.publish_delivery_report(internal_message_id, internal_status)

    @inlineCallbacks
    def setup_transport(self):
        log.msg('Starting the OperaInboundTransport config: %s' %
            self.transport_name)

        dbindex = get_deploy_int(self._amqp_client.vhost)
        redis_config = self.config.get('redis', {})
        self.r_server = yield redis.Redis(db=dbindex, **redis_config)
        self.r_prefix = "%(transport_name)s@%(url)s" % self.config

        self.proxy = xmlrpc.Proxy(self.opera_url)
        self.default_values = {
            'Service': self.opera_service,
            'Password': self.opera_password,
            'Channel': self.opera_channel,
        }

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
            content = xmlrpc.Binary(content.encode('utf-8'))

        xmlrpc_payload['Numbers'] = message['to_addr']
        xmlrpc_payload['SMSText'] = content
        xmlrpc_payload['Delivery'] = delivery
        xmlrpc_payload['Expiry'] = expiry
        xmlrpc_payload['Priority'] = priority
        xmlrpc_payload['Receipt'] = receipt

        log.msg("Sending SMS via Opera: %s" % xmlrpc_payload)

        d = self.proxy.callRemote('EAPIGateway.SendSMS',
            xmlrpc_payload)
        d.addErrback(self.handle_outbound_message_failure)

        proxy_response = yield d

        log.msg("Proxy response: %s" % proxy_response)
        transport_message_id = proxy_response['Identifier']

        self.set_message_id_for_identifier(transport_message_id,
            message['message_id'])

        yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=transport_message_id)

    def handle_outbound_message_failure(self, failure):
        """
        Decide what to do on certain failure cases.
        """
        if failure.check(xmlrpc.Fault):
            # If the XML-RPC service isn't behaving properly
            raise TemporaryFailure(failure)
        elif failure.check(ValueError):
            # If the HTTP protocol returns something other than 200
            raise PermanentFailure(failure)
        else:
            # Unspecified
            raise failure

    @inlineCallbacks
    def teardown_transport(self):
        log.msg("Stopping the OperaOutboundTransport: %s" %
            self.transport_name)
        yield self.web_resource.loseConnection()
