# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from urlparse import parse_qs

from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from twisted.web import xmlrpc

from vumi.utils import http_request, http_request_full
from vumi.transports.failures import PermanentFailure, TemporaryFailure
from vumi.transports.opera import OperaTransport
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import VumiTestCase


class FakeXMLRPCService(object):
    def __init__(self, callback):
        self.callback = callback

    def callRemote(self, *args, **kwargs):
        return maybeDeferred(self.callback, *args, **kwargs)


class TestOperaTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(
            TransportHelper(OperaTransport, mobile_addr='27761234567'))
        self.transport = yield self.tx_helper.get_transport({
            'url': 'http://testing.domain',
            'channel': 'channel',
            'service': 'service',
            'password': 'password',
            'web_receipt_path': '/receipt.xml',
            'web_receive_path': '/receive.xml',
            'web_port': 0,
        })

    @inlineCallbacks
    def test_receipt_processing(self):
        """it should be able to process an incoming XML receipt via HTTP"""

        identifier = '001efc31'
        message_id = '123456'
        # prime redis to match the incoming identifier to an
        # internal message id
        yield self.transport.set_message_id_for_identifier(
            identifier, message_id)

        xml_data = """
        <?xml version="1.0"?>
        <!DOCTYPE receipts>
        <receipts>
          <receipt>
            <msgid>26567958</msgid>
            <reference>%s</reference>
            <msisdn>+27123456789</msisdn>
            <status>D</status>
            <timestamp>20080831T15:59:24</timestamp>
            <billed>NO</billed>
          </receipt>
        </receipts>
        """.strip() % identifier
        yield http_request(
            self.transport.get_transport_url('receipt.xml'), xml_data)
        self.assertEqual([], self.tx_helper.get_dispatched_failures())
        self.assertEqual([], self.tx_helper.get_dispatched_inbound())
        [event] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['message_type'], 'event')
        self.assertEqual(event['event_type'], 'delivery_report')
        self.assertEqual(event['user_message_id'], message_id)

    @inlineCallbacks
    def test_incoming_sms_processing_urlencoded(self):
        """
        it should be able to process in incoming sms as XML delivered via HTTP
        """

        xml_data = (
            'XmlMsg=%3C%3Fxml%20version%3D%221.0%22%3F%3E%0A%3C!DOCTYPE%20bspo'
            'stevent%3E%0A%3Cbspostevent%3E%0A%20%20%3Cfield%20name%3D%22MORef'
            'erence%22%20type%20%3D%20%22string%22%3E478535078%3C/field%3E%0A%'
            '20%20%3Cfield%20name%3D%22RemoteNetwork%22%20type%20%3D%20%22stri'
            'ng%22%3Emtn-za%3C/field%3E%0A%20%20%3Cfield%20name%3D%22BSDate-to'
            'morrow%22%20type%20%3D%20%22string%22%3E20120317%3C/field%3E%0A%2'
            '0%20%3Cfield%20name%3D%22BSDate-today%22%20type%20%3D%20%22string'
            '%22%3E20120316%3C/field%3E%0A%20%20%3Cfield%20name%3D%22ReceiveDa'
            'te%22%20type%20%3D%20%22date%22%3E2012-03-16%2011:50:04%20%2B0000'
            '%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Local%22%20type%20%3D%2'
            '0%22string%22%3E*32323%3C/field%3E%0A%20%20%3Cfield%20name%3D%22C'
            'lientID%22%20type%20%3D%20%22string%22%3E4%3C/field%3E%0A%20%20%3'
            'Cfield%20name%3D%22ChannelID%22%20type%20%3D%20%22string%22%3E176'
            '%3C/field%3E%0A%20%20%3Cfield%20name%3D%22MessageID%22%20type%20%'
            '3D%20%22string%22%3E1487577162%3C/field%3E%0A%20%20%3Cfield%20nam'
            'e%3D%22Prefix%22%20type%20%3D%20%22string%22%3E%3C/field%3E%0A%20'
            '%20%3Cfield%20name%3D%22ClientName%22%20type%20%3D%20%22string%22'
            '%3EPraekelt%3C/field%3E%0A%20%20%3Cfield%20name%3D%22MobileDevice'
            '%22%20type%20%3D%20%22string%22%3E%3C/field%3E%0A%20%20%3Cfield%2'
            '0name%3D%22BSDate-yesterday%22%20type%20%3D%20%22string%22%3E2012'
            '0315%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Remote%22%20type%20'
            '%3D%20%22string%22%3E%2B27831234567%3C/field%3E%0A%20%20%3Cfield%'
            '20name%3D%22MobileNetwork%22%20type%20%3D%20%22string%22%3Emtn-za'
            '%3C/field%3E%0A%20%20%3Cfield%20name%3D%22State%22%20type%20%3D%2'
            '0%22string%22%3E9%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Mobile'
            'Number%22%20type%20%3D%20%22string%22%3E%2B27831234567%3C/field%3'
            'E%0A%20%20%3Cfield%20name%3D%22Text%22%20type%20%3D%20%22string%2'
            '2%3EHerb01%20spice01%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Ser'
            'viceID%22%20type%20%3D%20%22string%22%3E30756%3C/field%3E%0A%20%2'
            '0%3Cfield%20name%3D%22RegType%22%20type%20%3D%20%22string%22%3ESM'
            'S%3C/field%3E%0A%20%20%3Cfield%20name%3D%22NewSubscriber%22%20typ'
            'e%20%3D%20%22string%22%3ENO%3C/field%3E%0A%20%20%3Cfield%20name%3'
            'D%22Subscriber%22%20type%20%3D%20%22string%22%3E%2B27831234567%3C'
            '/field%3E%0A%20%20%3Cfield%20name%3D%22id%22%20type%20%3D%20%22st'
            'ring%22%3E3361920%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Parsed'
            '%22%20type%20%3D%20%22string%22%3E%3C/field%3E%0A%20%20%3Cfield%2'
            '0name%3D%22ServiceName%22%20type%20%3D%20%22string%22%3ERobertson'
            '%26%238217%3Bs%20Herb%20%26amp%3B%20Spices%20Promo%3C/field%3E%0A'
            '%20%20%3Cfield%20name%3D%22BSDate-thisweek%22%20type%20%3D%20%22s'
            'tring%22%3E20120312%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Serv'
            'iceEndDate%22%20type%20%3D%20%22string%22%3E2012-12-31%2003:06:00'
            '%20%2B0200%3C/field%3E%0A%20%20%3Cfield%20name%3D%22Now%22%20type'
            '%20%3D%20%22date%22%3E2012-03-16%2011:50:05%20%2B0000%3C/field%3E'
            '%0A%3C/bspostevent%3E%0A')

        resp = yield http_request(
            self.transport.get_transport_url('receive.xml'), xml_data)

        self.assertEqual([], self.tx_helper.get_dispatched_failures())
        self.assertEqual([], self.tx_helper.get_dispatched_events())
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['message_id'], '1487577162')
        self.assertEqual(msg['to_addr'], '32323')
        self.assertEqual(msg['from_addr'], '+27831234567')
        self.assertEqual(msg['content'], 'Herb01 spice01')
        self.assertEqual(msg['transport_metadata'], {
            'provider': 'mtn-za'
        })

        self.assertEqual(resp, parse_qs(xml_data)['XmlMsg'][0])

    @inlineCallbacks
    def test_incoming_sms_processing(self):
        """
        it should be able to process in incoming sms as XML delivered via HTTP
        """

        xml_data = """
        <?xml version="1.0"?>
        <!DOCTYPE bspostevent>
        <bspostevent>
          <field name="MOReference" type = "string">282341913</field>
          <field name="IsReceipt" type = "string">NO</field>
          <field name="RemoteNetwork" type = "string">mtn-za</field>
          <field name="BSDate-tomorrow" type = "string">20100605</field>
          <field name="BSDate-today" type = "string">20100604</field>
          <field name="ReceiveDate" type = "date">
                 2010-06-04 15:51:25 +0000</field>
          <field name="Local" type = "string">*32323</field>
          <field name="ClientID" type = "string">4</field>
          <field name="ChannelID" type = "string">111</field>
          <field name="MessageID" type = "string">373736741</field>
          <field name="ReceiptStatus" type = "string"></field>
          <field name="Prefix" type = "string"></field>
          <field name="ClientName" type = "string">Praekelt</field>
          <field name="MobileDevice" type = "string"></field>
          <field name="BSDate-yesterday" type = "string">20100603</field>
          <field name="Remote" type = "string">+27831234567</field>
          <field name="State" type = "string">5</field>
          <field name="MobileNetwork" type = "string">mtn-za</field>
          <field name="MobileNumber" type = "string">+27831234567</field>
          <field name="Text" type = "string">Hello World</field>
          <field name="ServiceID" type = "string">20222</field>
          <field name="RegType" type = "string">1</field>
          <field name="NewSubscriber" type = "string">NO</field>
          <field name="Subscriber" type = "string">+27831234567</field>
          <field name="Parsed" type = "string"></field>
          <field name="ServiceName" type = "string">Prktl Vumi</field>
          <field name="BSDate-thisweek" type = "string">20100531</field>
          <field name="ServiceEndDate" type = "string">
                 2010-06-30 07:47:00 +0200</field>
          <field name="Now" type = "date">2010-06-04 15:51:27 +0000</field>
        </bspostevent>
        """.strip()

        resp = yield http_request(
            self.transport.get_transport_url('receive.xml'), xml_data)

        self.assertEqual([], self.tx_helper.get_dispatched_failures())
        self.assertEqual([], self.tx_helper.get_dispatched_events())
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['message_id'], '373736741')
        self.assertEqual(msg['to_addr'], '32323')
        self.assertEqual(msg['from_addr'], '+27831234567')
        self.assertEqual(msg['content'], 'Hello World')
        self.assertEqual(msg['transport_metadata'], {
            'provider': 'mtn-za'
        })

        self.assertEqual(resp, xml_data)

    @inlineCallbacks
    def test_incoming_sms_no_data(self):
        resp = yield http_request_full(
            self.transport.get_transport_url('receive.xml'), None)

        self.assertEqual([], self.tx_helper.get_dispatched_failures())
        self.assertEqual([], self.tx_helper.get_dispatched_events())
        self.assertEqual([], self.tx_helper.get_dispatched_inbound())

        self.assertEqual(resp.code, 400)
        self.assertEqual(resp.delivered_body, "XmlMsg missing.")

    @inlineCallbacks
    def test_incoming_sms_partial_data(self):

        xml_data = """
        <?xml version="1.0"?>
        <!DOCTYPE bspostevent>
        <bspostevent>
          <field name="MOReference" type = "string">282341913</field>
          <field name="IsReceipt" type = "string">NO</field>
          <field name="RemoteNetwork" type = "string">mtn-za</field>
          <field name="BSDate-tomorrow" type = "string">20100605</field>
          <field name="BSDate-today" type = "string">20100604</field>
          <field name="ReceiveDate" type = "date">
                 2010-06-04 15:51:25 +0000</field>
          <field name="ClientID" type = "string">4</field>
          <field name="ChannelID" type = "string">111</field>
          <field name="MessageID" type = "string">373736741</field>
          <field name="ReceiptStatus" type = "string"></field>
          <field name="Prefix" type = "string"></field>
          <field name="ClientName" type = "string">Praekelt</field>
          <field name="MobileDevice" type = "string"></field>
          <field name="BSDate-yesterday" type = "string">20100603</field>
          <field name="Remote" type = "string">+27831234567</field>
          <field name="State" type = "string">5</field>
          <field name="MobileNetwork" type = "string">mtn-za</field>
          <field name="MobileNumber" type = "string">+27831234567</field>
          <field name="Text" type = "string">Hello World</field>
          <field name="ServiceID" type = "string">20222</field>
          <field name="RegType" type = "string">1</field>
          <field name="NewSubscriber" type = "string">NO</field>
          <field name="Subscriber" type = "string">+27831234567</field>
          <field name="Parsed" type = "string"></field>
          <field name="ServiceName" type = "string">Prktl Vumi</field>
          <field name="BSDate-thisweek" type = "string">20100531</field>
          <field name="ServiceEndDate" type = "string">
                 2010-06-30 07:47:00 +0200</field>
          <field name="Now" type = "date">2010-06-04 15:51:27 +0000</field>
        </bspostevent>
        """.strip()

        resp = yield http_request_full(
            self.transport.get_transport_url('receive.xml'), xml_data)

        self.assertEqual([], self.tx_helper.get_dispatched_failures())
        self.assertEqual([], self.tx_helper.get_dispatched_events())
        self.assertEqual([], self.tx_helper.get_dispatched_inbound())

        self.assertEqual(resp.code, 400)
        self.assertEqual(resp.delivered_body, "Missing field: Local")

    @inlineCallbacks
    def test_outbound_ok(self):
        """
        Outbound message we send should hit the XML-RPC service with the
        correct parameters
        """

        def _cb(method_called, xmlrpc_payload):
            self.assertEqual(method_called, 'EAPIGateway.SendSMS')
            self.assertEqual(xmlrpc_payload['Priority'], 'standard')
            self.assertEqual(xmlrpc_payload['SMSText'], 'hello world')
            self.assertEqual(xmlrpc_payload['Service'], 'service')
            self.assertEqual(xmlrpc_payload['Receipt'], 'Y')
            self.assertEqual(xmlrpc_payload['MaxSegments'], 9)
            self.assertEqual(xmlrpc_payload['Numbers'], '27761234567')
            self.assertEqual(xmlrpc_payload['Password'], 'password')
            self.assertEqual(xmlrpc_payload['Channel'], 'channel')
            now = datetime.utcnow()
            tomorrow = now + timedelta(days=1)
            self.assertEqual(xmlrpc_payload['Expiry'].hour, tomorrow.hour)
            self.assertEqual(xmlrpc_payload['Expiry'].minute, tomorrow.minute)
            self.assertEqual(xmlrpc_payload['Expiry'].date(), tomorrow.date())

            self.assertEqual(xmlrpc_payload['Delivery'].hour, now.hour)
            self.assertEqual(xmlrpc_payload['Delivery'].minute, now.minute)
            self.assertEqual(xmlrpc_payload['Delivery'].date(), now.date())

            return {
                'Identifier': 'abc123'
            }

        self.transport.proxy = FakeXMLRPCService(_cb)

        msg = yield self.tx_helper.make_dispatch_outbound('hello world')

        self.assertEqual(self.tx_helper.get_dispatched_failures(), [])
        self.assertEqual(self.tx_helper.get_dispatched_inbound(), [])
        [event_msg] = self.tx_helper.get_dispatched_events()
        self.assertEqual(event_msg['message_type'], 'event')
        self.assertEqual(event_msg['event_type'], 'ack')
        self.assertEqual(event_msg['sent_message_id'], 'abc123')
        # test that we've properly linked the identifier to our
        # internal id of the given message
        self.assertEqual(
            (yield self.transport.get_message_id_for_identifier('abc123')),
            msg['message_id'])

    @inlineCallbacks
    def test_outbound_ok_with_metadata(self):
        """
        Outbound message we send should hit the XML-RPC service with the
        correct parameters
        """

        fixed_date = datetime(2011, 1, 1, 0, 0, 0)

        def _cb(method_called, xmlrpc_payload):
            self.assertEqual(xmlrpc_payload['Delivery'], fixed_date)
            self.assertEqual(xmlrpc_payload['Expiry'],
                             fixed_date + timedelta(hours=1))
            self.assertEqual(xmlrpc_payload['Priority'], 'high')
            self.assertEqual(xmlrpc_payload['Receipt'], 'N')
            return {
                'Identifier': 'abc123'
            }

        self.transport.proxy = FakeXMLRPCService(_cb)

        yield self.tx_helper.make_dispatch_outbound("hi", transport_metadata={
            'deliver_at': fixed_date,
            'expire_at': fixed_date + timedelta(hours=1),
            'priority': 'high',
            'receipt': 'N',
        })

    @inlineCallbacks
    def test_outbound_temporary_failure(self):
        """
        if for some reason the delivery of the SMS to opera crashes it
        shouldn't ACK the message over AMQ but leave it for a retry later
        """

        def _cb(*args, **kwargs):
            """
            Callback handler that raises an error when called
            """
            return defer.fail(xmlrpc.Fault(503, 'oh noes!'))

        # monkey patch so we can mock errors happening remotely
        self.transport.proxy = FakeXMLRPCService(_cb)

        # send a message to the transport which'll hit the FakeXMLRPCService
        # and as a result raise an error
        yield self.tx_helper.make_dispatch_outbound("hello world")

        [twisted_failure] = self.flushLoggedErrors(TemporaryFailure)
        logged_failure = twisted_failure.value
        self.assertEqual(logged_failure.failure_code, 'temporary')

        self.assertEqual(self.tx_helper.get_dispatched_events(), [])
        self.assertEqual(self.tx_helper.get_dispatched_inbound(), [])
        [failure] = self.tx_helper.get_dispatched_failures()
        self.assertEqual(failure['failure_code'], 'temporary')
        original_msg = failure['message']
        self.assertEqual(original_msg['to_addr'], '27761234567')
        self.assertEqual(original_msg['from_addr'], '9292')
        self.assertEqual(original_msg['content'], 'hello world')

    @inlineCallbacks
    def test_outbound_permanent_failure(self):
        """
        if for some reason the Opera XML-RPC service gives us something
        other than a 200 response it should consider it a permanent
        failure
        """

        def _cb(*args, **kwargs):
            """
            Callback handler that raises an error when called
            """
            return defer.fail(ValueError(402, 'Payment Required'))

        # monkey patch so we can mock errors happening remotely
        self.transport.proxy = FakeXMLRPCService(_cb)

        # send a message to the transport which'll hit the FakeXMLRPCService
        # and as a result raise an error
        msg = yield self.tx_helper.make_dispatch_outbound("hi")

        [twisted_failure] = self.flushLoggedErrors(PermanentFailure)
        logged_failure = twisted_failure.value
        self.assertEqual(logged_failure.failure_code, 'permanent')

        [failure] = self.tx_helper.get_dispatched_failures()
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(failure['failure_code'], 'permanent')
        self.assertEqual(nack['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_outbound_unicode_encoding(self):
        """
        Opera supports unicode encoded SMS messages as long as they
        encoded as xmlrpc.Binary, test that.
        """

        content = u'üïéßø'

        def _cb(method_called, xmlrpc_payload):
            self.assertEqual(xmlrpc_payload['SMSText'],
                xmlrpc.Binary(content.encode('utf-8')))
            return {'Identifier': '1'}

        self.transport.proxy = FakeXMLRPCService(_cb)
        yield self.tx_helper.make_dispatch_outbound(content)
