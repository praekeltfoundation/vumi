from twisted.internet.defer import inlineCallbacks
from twisted.internet import endpoints, tcp
from twisted.internet.task import Clock
from twisted.web.xmlrpc import Proxy

from vumi.message import TransportUserMessage
from vumi.transports.mtn_rwanda.mtn_rwanda_ussd import (
        MTNRwandaUSSDTransport, MTNRwandaXMLRPCResource)
from vumi.transports.tests.utils import TransportTestCase


class MTNRwandaUSSDTransportTestCase(TransportTestCase):

    transport_class = MTNRwandaUSSDTransport
    transport_name = 'test_mtn_rwanda_ussd_transport'

    EXPECTED_INBOUND_PAYLOAD = {
            'message_id': '',
            'content': '',
            'from_addr': '',    # msisdn
            'to_addr': '',      # service code
            'transport_name': transport_name,
            'transport_type': 'ussd',
            'transport_metadata': {
                'mtn_rwanda_ussd': {
                    'transaction_id': '0001',
                    'transaction_time': '20060723T14:08:55',
                    },
                },
            }

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
        self.assertIsInstance(self.transport.endpoint,
                endpoints.TCP4ServerEndpoint)
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

    def mk_reply(self, request_msg, reply_content, continue_session=True):
        request_msg = TransportUserMessage(**request_msg.payload)
        return request_msg.reply(reply_content, continue_session)

    @inlineCallbacks
    def test_inbound_request_and_reply(self):
        address = self.transport.xmlrpc_server.getHost()
        url = 'http://' + address.host + ':' + str(address.port) + '/'
        proxy = Proxy(url)
        x = proxy.callRemote('handleUSSD',
                         'TransactionId', '0001',
                         'USSDServiceCode', '543',
                         'USSDRequestString', '14321*1000#',
                         'MSISDN', '275551234',
                         'USSDEncoding', 'GSM0338',      # Optional
                         'TransactionTime', '20060723T14:08:55')
        [msg] = yield self.wait_for_dispatched_messages(1)
        yield self.assert_inbound_message(msg,
                                          from_addr='275551234',
                                          to_addr='543',
                                          content='14321*1000#')
        expected_reply = {'MSISDN': '275551234',
                          'TransactionId': '0001',
                          'TransactionTime': '20060723T14:08:55',
                          'USSDEncoding': 'GSM0338',
                          'USSDResponseString': 'Test message',
                          'USSDServiceCode': '543'}

        reply = self.mk_reply(msg, expected_reply['USSDResponseString'])

        self.dispatch(reply)
        received_text = yield x
        self.assertEqual(expected_reply, received_text)



    '''
    @inlineCallbacks
    def test_inbound_request_missing_params(self):
        address = self.transport.xmlrpc_server.getHost()
        url = 'http://'+address.host+':'+str(address.port)+'/'
        proxy = Proxy(url)
        x = yield proxy.callRemote('handleUSSD',
                         'TransactionId', '0001',
                         'USSDServiceCode', '543',
                         'USSDRequestString', '14321*1000#',
                         'USSDEncoding', 'GSM0338',      # Optional
                         'response', 'false',            # Optional
                         'TransactionTime', '20060723T14:08:55')
        [msg]= yield self.wait_for_dispatched_messages(1)
        yield self.assert_inbound_message(msg,
                                          from_addr='275551234',
                                          to_addr='543',
                                          content='14321*1000#')
        print "On missing params, I got:", x
    '''
