# -*- test-case-name: vumi.transports.opera.tests.test_opera -*-
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from urlparse import parse_qs

from twisted.python import log
from twisted.web import xmlrpc, http
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks

from vumi.utils import normalize_msisdn
from vumi.transports import Transport
from vumi.transports.failures import TemporaryFailure, PermanentFailure
from vumi.transports.opera import utils
from vumi.components.session import SessionManager


class BadRequestError(Exception):
    """
    An exception we can throw while parsing a request to return a 400 response.
    """


def get_receipts_xml(content):
    if content.startswith('<'):
        return content
    decoded = parse_qs(content)
    if 'XmlMsg' not in decoded:
        raise BadRequestError("XmlMsg missing.")
    return decoded['XmlMsg'][0]


class OperaHealthResource(Resource):
    isLeaf = True

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return "OK"


class OperaReceiptResource(Resource):

    def __init__(self, callback):
        self.callback = callback
        Resource.__init__(self)

    def render_POST(self, request):
        content = get_receipts_xml(request.content.read())
        receipts = utils.parse_receipts_xml(content)
        for receipt in receipts:
            self.callback(receipt)

        request.setResponseCode(http.OK)
        return ''


class OperaReceiveResource(Resource):

    def __init__(self, callback):
        self.callback = callback
        Resource.__init__(self)

    def render_POST(self, request):
        try:
            content = get_receipts_xml(request.content.read())
            sms = utils.parse_post_event_xml(content)
            for field in [
                    'Local', 'Remote', 'Text', 'MessageID', 'MobileNetwork']:
                if field not in sms:
                    raise BadRequestError("Missing field: %s" % (field,))
        except BadRequestError as err:
            request.setResponseCode(http.BAD_REQUEST)
            request.setHeader('Content-Type', 'text/plain; charset=utf8')
            return err.args[0]
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
    Opera transport.

    See https://dragon.sa.operatelecom.com:1089/ for documentation
    on the Opera XML-RPC interface.

    Configuration options:

    :type message_id_lifetime: int
    :param message_id_lifetime:
        Seconds message ids should be kept for before expiring. Once
        an id expires, delivery reports can no longer be associated
        with the original message id. Default is one week.
    :type web_receipt_path: str
    :param web_receipt_path:
        Path part of JSON reply URL (should match value given to Opera).
        E.g. /api/v1/sms/opera/receipt.json
    :type web_receive_path: str
    :param web_receive_path:
        Path part of XML reply URL (should match value given to Opera).
        E.g. /api/v1/sms/opera/receive.xml
    :type web_port: int
    :param web_port:
        Port the transport listens to for responses from Opera.
        Affects both web_receipt_path and web_receive_path.
    :type url: str
    :param url:
        Opera XML-RPC gateway. E.g.
        https://dragon.sa.operatelecom.com:1089/Gateway
    :type channel: str
    :param channel:
        Opera channel number.
    :type password: str
    :param password:
        Opera password.
    :type service: str
    :param service:
        Opera service number.
    :type max_segments: int
    :param max_segments:
        Maximum number of segments to allow messages to be broken
        into. Default is 9. Minimum is 1. Maximum is 9. Note: Opera's
        own default is 1. This transport defaults to 9 to minimise the
        possibility of message sends failing.
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
        self.max_segments = self.config.get('max_segments', 9)
        self.r_config = self.config.get('redis_manager', {})
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
        return self.session_manager.create_session(
            identifier, message_id=message_id)

    def get_message_id_for_identifier(self, identifier):
        """
        Get an internal message id for a given identifier

        :type identifier: str
        :param identifier:
            The message id we originally got from Opera when the message
            was accepted for delivery.

        """
        d = self.session_manager.load_session(identifier)
        return d.addCallback(lambda s: s.get('message_id', None))

    @inlineCallbacks
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
        message_id = yield self.get_message_id_for_identifier(
            receipt.reference)
        yield self.publish_delivery_report(message_id, internal_status)

    @inlineCallbacks
    def setup_transport(self):
        log.msg('Starting the OperaInboundTransport config: %s' %
            self.transport_name)
        r_prefix = "%(transport_name)s@%(url)s" % self.config
        self.session_manager = yield SessionManager.from_redis_config(
            self.r_config, r_prefix, self.message_id_lifetime)

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

    def get_transport_url(self, suffix=''):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.

        This is mostly useful in tests, and probably shouldn't be used
        in non-test code, because the API might live behind a load
        balancer or proxy.
        """
        addr = self.web_resource.getHost()
        return "http://%s:%s/%s" % (addr.host, addr.port, suffix.lstrip('/'))

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
        xmlrpc_payload['MaxSegments'] = self.max_segments

        log.msg("Sending SMS via Opera: %s" % xmlrpc_payload)

        d = self.proxy.callRemote('EAPIGateway.SendSMS',
            xmlrpc_payload)
        d.addErrback(self.handle_outbound_message_failure, message)

        proxy_response = yield d

        log.msg("Proxy response: %s" % proxy_response)
        transport_message_id = proxy_response['Identifier']

        yield self.set_message_id_for_identifier(
            transport_message_id, message['message_id'])

        yield self.publish_ack(
                user_message_id=message['message_id'],
                sent_message_id=transport_message_id)

    @inlineCallbacks
    def handle_outbound_message_failure(self, failure, message):
        """
        Decide what to do on certain failure cases.
        """
        if failure.check(xmlrpc.Fault):
            # If the XML-RPC service isn't behaving properly
            raise TemporaryFailure(failure)
        elif failure.check(ValueError):
            # If the HTTP protocol returns something other than 200
            yield self.publish_nack(message['message_id'], str(failure.value))
            raise PermanentFailure(failure)
        else:
            # Unspecified
            yield self.publish_nack(message['message_id'], str(failure.value))
            raise failure

    @inlineCallbacks
    def teardown_transport(self):
        log.msg("Stopping the OperaOutboundTransport: %s" %
            self.transport_name)
        yield self.web_resource.loseConnection()
        yield self.session_manager.stop()
