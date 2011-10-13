from datetime import datetime, timedelta
from StringIO import StringIO
import os

import iso8601
from twisted.trial import unittest
from twisted.python import failure
from twisted.internet import defer
from twisted.internet.defer import inlineCallbacks
from twisted.web.test.test_web import DummyRequest
from twisted.web import xmlrpc

os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'

from vumi.message import Message, TransportUserMessage
from vumi.transports.opera import opera
from vumi.tests.utils import TestPublisher, FakeRedis
from vumi.utils import http_request
from vumi.transports.opera.tests.test_opera_stubs import FakeXMLRPCService
from vumi.transports.opera import OperaOutboundTransport, OperaInboundTransport
from vumi.transports.tests.test_base import TransportTestCase
# from vumi.webapp.api.models import *


class OperaTransportTestCase(TransportTestCase):

    @inlineCallbacks
    def setUp(self):
        self.port = 9999
        self.host = "localhost"
        self.url = 'http://%s:%s' % (self.host, self.port)
        yield super(OperaTransportTestCase, self).setUp()

    def mk_transport(self, cls=OperaOutboundTransport, **config):
        default_config = {
            'url': 'http://testing.domain',
            'channel': 'channel',
            'service': 'service',
            'password': 'password',
        }
        default_config.update(config)
        return self.get_transport(default_config, cls)

    def mk_msg(self, **kwargs):
        defaults = {
            'to_addr': '27761234567',
            'from_addr': '27761234567',
            'content': 'hello world',
            'transport_name': self.transport_name,
            'transport_type': 'sms',
            'transport_metadata': {}
        }
        defaults.update(kwargs)
        return TransportUserMessage(**defaults)
    
    @inlineCallbacks
    def test_receipt_processing(self):
        """it should be able to process an incoming XML receipt via HTTP"""
        transport = yield self.mk_transport(cls=OperaInboundTransport, 
            web_receipt_path='/receipt.xml', web_receive_path='/receive.xml',
            web_port=self.port)
        
        identifier = '001efc31'
        message_id = '123456'
        transport.r_server = FakeRedis()
        # prime redis to match the incoming identifier to an internal message id
        transport.set_message_id_for_identifier(identifier, message_id)


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
        resp = yield http_request('%s/receipt.xml' % self.url, xml_data)
        self.assertEqual([], self.get_dispatched_failures())
        self.assertEqual([], self.get_dispatched_messages())
        [event] = self.get_dispatched_events()
        self.assertEqual(event['delivery_status'], 'delivered')
        self.assertEqual(event['message_type'], 'event')
        self.assertEqual(event['event_type'], 'delivery_report')
        # this is failing because I need to stash the mapping in Redis
        self.assertEqual(event['transport_message_id'], message_id)
    
    @inlineCallbacks
    def test_incoming_sms_processing(self):
        """
        it should be able to process in incoming sms as XML delivered via HTTP
        """
        transport = yield self.mk_transport(cls=OperaInboundTransport, 
            web_receipt_path='/receipt.xml', web_receive_path='/receive.xml',
            web_port=self.port)

        xml_data = """
        <?xml version="1.0"?>
        <!DOCTYPE bspostevent>
        <bspostevent>
          <field name="MOReference" type = "string">282341913</field>
          <field name="IsReceipt" type = "string">NO</field>
          <field name="RemoteNetwork" type = "string">mtn-za</field>
          <field name="BSDate-tomorrow" type = "string">20100605</field>
          <field name="BSDate-today" type = "string">20100604</field>
          <field name="ReceiveDate" type = "date">2010-06-04 15:51:25 +0000</field>
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
          <field name="ServiceEndDate" type = "string">2010-06-30 07:47:00 +0200</field>
          <field name="Now" type = "date">2010-06-04 15:51:27 +0000</field>
        </bspostevent>
        """.strip()

        resp = yield http_request('%s/receive.xml' % self.url, xml_data)

        self.assertEqual([], self.get_dispatched_failures())
        self.assertEqual([], self.get_dispatched_events())
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['message_id'], '373736741')
        self.assertEqual(msg['to_addr'], '32323')
        self.assertEqual(msg['from_addr'], '+27831234567')
        self.assertEqual(msg['content'], 'Hello World')
        self.assertEqual(msg['transport_metadata'], {
            'provider': 'mtn-za'
        })

        self.assertEqual(resp, xml_data)
    
    @inlineCallbacks
    def test_outbound_ok(self):
        """
        Outbound message we send should hit the XML-RPC service with the correct
        parameters
        """

        transport = yield self.mk_transport()

        def _cb(method_called, xmlrpc_payload):
            self.assertEqual(method_called, 'EAPIGateway.SendSMS')
            self.assertEqual(xmlrpc_payload['Priority'], 'standard')
            self.assertEqual(xmlrpc_payload['SMSText'], 'hello world')
            self.assertEqual(xmlrpc_payload['Service'], 'service')
            self.assertEqual(xmlrpc_payload['Receipt'], 'Y')
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
        
        transport.proxy = FakeXMLRPCService(_cb)

        msg = self.mk_msg()
        yield self.dispatch(msg, 
            rkey='%s.outbound' % self.transport_name)
        
        self.assertEqual(self.get_dispatched_failures(), [])
        self.assertEqual(self.get_dispatched_messages(), [])
        [event_msg] = self.get_dispatched_events()
        self.assertEqual(event_msg['message_type'], 'event')
        self.assertEqual(event_msg['event_type'], 'ack')
        self.assertEqual(event_msg['sent_message_id'], 'abc123')
        # test that we've properly linked the identifier to our
        # internal id of the given message
        self.assertEqual(
            transport.get_message_id_for_identifier('abc123'),
            msg['message_id'])
    

    @inlineCallbacks
    def test_outbound_ok_with_metadata(self):
        """
        Outbound message we send should hit the XML-RPC service with the correct
        parameters
        """

        transport = yield self.mk_transport()
        fixed_date = datetime(2011,1,1,0,0,0)
        
        def _cb(method_called, xmlrpc_payload):
            self.assertEqual(xmlrpc_payload['Delivery'], fixed_date)
            self.assertEqual(xmlrpc_payload['Expiry'], fixed_date + timedelta(hours=1))
            self.assertEqual(xmlrpc_payload['Priority'], 'high')
            self.assertEqual(xmlrpc_payload['Receipt'], 'N')
            return {
                'Identifier': 'abc123'
            }
        
        transport.proxy = FakeXMLRPCService(_cb)

        msg = yield self.dispatch(self.mk_msg(transport_metadata={
            'deliver_at': fixed_date,
            'expire_at': fixed_date + timedelta(hours=1),
            'priority': 'high',
            'receipt': 'N',
            }), 
            rkey='%s.outbound' % self.transport_name)
    

    @inlineCallbacks
    def test_outbound_crash(self):
        """
        if for some reason the delivery of the SMS to opera crashes it
        shouldn't ACK the message over AMQ but leave it for a retry later
        """

        transport = yield self.mk_transport()

        def _cb(*args, **kwargs):
            """
            Callback handler that raises an error when called
            """
            return defer.fail(xmlrpc.Fault(503, 'oh noes!'))
        
        # monkey patch so we can mock errors happening remotely
        transport.proxy = FakeXMLRPCService(_cb)

        # send a message to the transport which'll hit the FakeXMLRPCService
        # and as a result raise an error
        msg = yield self.dispatch(self.mk_msg(),
            rkey='%s.outbound' % self.transport_name)

        self.assertEqual(self.get_dispatched_events(), [])
        self.assertEqual(self.get_dispatched_messages(), [])
        [failure] = self.get_dispatched_failures()
        original_msg = failure['message']
        self.assertEqual(original_msg['to_addr'], '27761234567')
        self.assertEqual(original_msg['from_addr'], '27761234567')
        self.assertEqual(original_msg['content'], 'hello world')
