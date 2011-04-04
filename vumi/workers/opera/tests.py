import os, iso8601
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'

from django.test.simple import DjangoTestSuiteRunner

from twisted.trial import unittest
from twisted.python import log
from twisted.web.test.test_web import DummyRequest
from twisted.web import http

from vumi.message import Message
from vumi.workers.opera import transport
from vumi.webapp.api.models import *

from StringIO import StringIO

class TestPublisher(object):
    """
    A test publisher that caches outbound messages in an internal queue
    for testing, instead of publishing over AMQP.
    """
    def __init__(self):
        self.queue = []
    
    def publish_message(self, message, **kwargs):
        self.queue.append((message, kwargs))
    

class OperaTransportTestCase(unittest.TestCase):
    
    def test_receipt_processing(self):
        """it should be able to process an incoming XML receipt via HTTP"""
        publisher = TestPublisher()
        resource = transport.OperaReceiptResource(publisher)
        request = DummyRequest('/api/v1/sms/opera/receipt.xml')
        request.content = StringIO("""
        <?xml version="1.0"?>
        <!DOCTYPE receipts>
        <receipts>
          <receipt>
            <msgid>26567958</msgid>
            <reference>001efc31</reference>
            <msisdn>+27123456789</msisdn>
            <status>D</status>
            <timestamp>20080831T15:59:24</timestamp>
            <billed>NO</billed>
          </receipt>
        </receipts>
        """.strip())
        response = resource.render_POST(request)
        self.assertEquals(publisher.queue.pop(), (Message(**{
                'transport_name': 'Opera',
                'transport_msg_id': '001efc31',
                'transport_status': 'D', # OK / delivered, opera specific
                'transport_delivered_at': datetime(2008,8,31,15,59,24),
            }), {
                'routing_key': 'sms.receipt.opera'
            })
        )
    
    def test_incoming_sms_processing(self):
        """it should be able to process in incoming sms as XML delivered via HTTP"""
        publisher = TestPublisher()
        resource = transport.OperaReceiveResource(publisher)
        request = DummyRequest('/api/v1/sms/opera/receive.xml')
        request.content = StringIO("""
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
        """.strip())
        response = resource.render_POST(request)
        self.assertEquals(publisher.queue.pop(), (Message(**{
                'to_msisdn': '*32323',
                'from_msisdn': '+27831234567',
                'message': 'Hello World',
                'transport_name': 'Opera',
                'received_at': iso8601.parse_date('2010-06-04 15:51:25 +0000'),
            }), {
                'routing_key': 'sms.inbound.opera.s32323' # * -> s
            }))
    
