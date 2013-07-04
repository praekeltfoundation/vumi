import xmlrpclib
from twisted.internet.defer import inlineCallbacks
from twisted.internet import endpoints, tcp, defer
from twisted.internet.task import Clock
from twisted.web.xmlrpc import Proxy

from vumi.transports.mtn_rwanda.mtn_rwanda_ussd import MTNRwandaUSSDTransport, MTNRwandaXMLRPCResource
from vumi.transports.tests.utils import TransportTestCase


class MTNRwandaUSSDTransportTestCase(TransportTestCase):

    transport_class = MTNRwandaUSSDTransport
    transport_name = 'test_mtn_rwanda_ussd_transport'

    ''' Irrelevant for now

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
    '''
    EXPECTED_INBOUND_PAYLOAD = {
            'message_id': '0',
            'content': '',
            'from_addr': '', # msisdn
            'to_addr': '', # service code
            'transport_name': transport_name,
            'transport_type': 'ussd',
            'transport_metadata': {
                'mtn_rwanda_ussd': {
                    'transaction_id': '0001',
                    'transaction_time': '20060723T14:08:55',
                    'response_flag': 'false',
                    },
                },
            }
    '''
    OUTBOUND_PAYLOAD = {
            'message_id': '0',
            'content': '',
            'from_addr': '', # Service code
            'to_addr': '543', # msisdn
            'transport_name': transport_name,
            'transport_type':'ussd',
            'transport_metadata': {
                'mtn_rwanda_ussd': {
                    'transaction_id': '0001',
                    'transaction_time': '1994-11-05T08:15:30-05:00',
                    'response_code': '0',
                    'action': 'end',
                    },
                },
            }
    '''
    @inlineCallbacks
    def setUp(self):
        """
        Create the server (i.e. vumi transport instance)
        """
        super(MTNRwandaUSSDTransportTestCase, self).setUp()
        self.clock = Clock()
        config = self.mk_config({
            'twisted_endpoint': 'tcp:port=0',
            'timeout': '30',
        })
        self.transport = yield self.get_transport(config)
        self.transport.callLater = self.clock.callLater

    def test_transport_creation(self):
        self.assertIsInstance(self.transport, MTNRwandaUSSDTransport)
        self.assertIsInstance(self.transport.endpoint, endpoints.TCP4ServerEndpoint)
        self.assertIsInstance(self.transport.xmlrpc_server, tcp.Port)
        self.assertIsInstance(self.transport.r, MTNRwandaXMLRPCResource)

    def test_transport_teardown(self):
        d = self.transport.teardown_transport()
        self.assertTrue(self.transport.xmlrpc_server.disconnecting)
        return d

    def assert_inbound_message(self, msg, **field_values):
        expected_payload = self.EXPECTED_INBOUND_PAYLOAD.copy()
        field_values['message_id'] = msg['message_id']
        expected_payload.update(field_values)
        for field, expected_value in expected_payload.iteritems():
            self.assertEqual(msg[field], expected_value)

    @inlineCallbacks
    def test_inbound_request(self):
        address = self.transport.xmlrpc_server.getHost()
        url = 'http://'+address.host+':'+str(address.port)+'/'
        proxy = Proxy(url)
        proxy.callRemote('handleUSSD',
                         'TransactionId', '0001',
                         'USSDServiceCode', '543',
                         'USSDRequestString', '14321*1000#',
                         'MSISDN', '275551234',
                         'USSDEncoding', 'GSM0338',      # Optional
                         'response', 'false',            # Optional
                         'TransactionTime', '20060723T14:08:55')
        [msg]= yield self.wait_for_dispatched_messages(1)
    	yield self.assert_inbound_message(msg,
                                          from_addr='275551234',
                                          to_addr='543',
                                          content='14321*1000#')

