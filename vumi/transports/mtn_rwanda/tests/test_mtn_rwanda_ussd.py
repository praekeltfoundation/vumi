from twisted.internet.defer import Deferred, inlineCallbacks

from vumi.transports.mtn_rwanda.mtn_rwanda_ussd import MTNRwandaUSSDTransport
from vumi.transports.tests.utils import TransportTestCase


class TestMTNRwandaUSSDTransportTestCase(TransportTestCase):

    transport_class = MTNRwandaUSSDTransport

    REQUEST_PARAMS = {
            'transaction_id': '0',
            'ussd_service_code': '100',
            'ussd_request_string': '',
            'msisdn': '',
            'response_flag': '',
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
            'transaction_time': '1994-11-05T08:15:30-05:00'
            'ussd_response_string': '',
            'response_code': '',
            'action': '',
            }
    RESPONSE_BODY = (
            "<USSDResponse>"
            "<TransactionId>%(transaction_id)s</TransactionId>"
            "<TransactionTime>%(transaction_time)s</TransactionTime>"
            "<USSDResponseString>%(ussd_response_string)s</USSDResponseString>"
            "<ResponseCode>%(response_code)s</ResponseCode>"
            "<action>%(action)s</action>"
            )

    # XXX: Expected Inbound Payload
    # XXX: Expected Outbound Payload


    @inlineCallbacks
    def setUp(self):
        """
        Create the server (i.e. vumi transport instance)
        """
        super(TestMTNRwandaUSSDTransportTestCase, self).setUp()

        config = {
                'server_hostname': '127.0.0.1',
                'server_port': self.get_server_port(),
                }
        self.transport = yield self.get_transport(config)


        @inlineCallbacks
        def tearDown(self):
            yield self.transport.teardown_transport()
            yield








