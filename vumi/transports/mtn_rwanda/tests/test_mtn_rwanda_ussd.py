from twisted.internet.defer import inlineCallbacks
from twisted.internet import endpoints, tcp, defer

from vumi.transports.mtn_rwanda.mtn_rwanda_ussd import MTNRwandaUSSDTransport, MTNRwandaXMLRPCResource
from vumi.transports.tests.utils import TransportTestCase


class MTNRwandaUSSDTransportTestCase(TransportTestCase):

    transport_class = MTNRwandaUSSDTransport
    transport_name = 'test_mtn_rwanda_ussd_transport'

    REQUEST_PARAMS = {
        'transaction_id': '0',
        'ussd_service_code': '100',
        'ussd_request_string': '',
        'msisdn': '',
        'response_flag': 'false',
        'transaction_time': '1994-11-05T08:15:30-05:00',
    }

    REQUEST_BODY = (
        "<USSDRequest>"
        "<TransactionId>%(transaction_id)s</TransactionId>"
        "<USSDServiceCode>%(ussd_service_code)s</USSDServiceCode>"
        "<USSDRequestString>%(ussd_request_string)s</USSDRequestString>"
        "<MSISDN>%(msisdn)s</MSISDN>"
        "<response>%(response_flag)s</response>"
        "<TransactionTime>%(transaction_time)s</TransactionTime>"
    )

    RESPONSE_PARAMS = {
        'transaction_id': '0',
        'transaction_time': '1994-11-05T08:15:30-05:00',
        'ussd_response_string': '',
        'response_code': '0',
        'action': 'end',
    }

    RESPONSE_BODY = (
        "<USSDResponse>"
        "<TransactionId>%(transaction_id)s</TransactionId>"
        "<TransactionTime>%(transaction_time)s</TransactionTime>"
        "<USSDResponseString>%(ussd_response_string)s</USSDResponseString>"
        "<ResponseCode>%(response_code)s</ResponseCode>"
        "<action>%(action)s</action>"
    )

    EXPECTED_INBOUND_PAYLOAD = {
            'message_id': '0',
            'content': '',
            'from_addr': '', # msisdn
            'to_addr': '', # service code
            'transport_name': transport_name,
            'transport_type': 'ussd',
            'transport_metadata': {
                'mtn_rwanda_ussd': {
                    'transaction_id': '0',
                    'transaction_time': '1994-11-05T08:15:30-05:00',
                    'response_flag': 'false',
                    },
                },
            }

    OUTBOUND_PAYLOAD = {
            'message_id': '0',
            'content': '',
            'from_addr': '', # Service code
            'to_addr': '', # msisdn
            'transport_name': transport_name,
            'transport_type':'ussd',
            'transport_metadata': {
                'mtn_rwanda_ussd': {
                    'transaction_id': '0',
                    'transaction_time': '1994-11-05T08:15:30-05:00',
                    'response_code': '0',
                    'action': 'end',
                    },
                },
            }

    @inlineCallbacks
    def setUp(self):
        """
        Create the server (i.e. vumi transport instance)
        """
        super(MTNRwandaUSSDTransportTestCase, self).setUp()

        config = self.mk_config({
            'twisted_endpoint': 'tcp:port=0',
            'timeout': '30',
        })
        self.transport = yield self.get_transport(config)

    def test_transport_creation(self):
        self.assertIsInstance(self.transport, MTNRwandaUSSDTransport)
        self.assertIsInstance(self.transport.endpoint, endpoints.TCP4ServerEndpoint)
        self.assertIsInstance(self.transport.xmlrpc_server, tcp.Port)
        self.assertIsInstance(self.transport.r, MTNRwandaXMLRPCResource)

    def test_transport_teardown(self):
        d = self.transport.teardown_transport()
        self.assertTrue(self.transport.xmlrpc_server.disconnecting)
        return d

