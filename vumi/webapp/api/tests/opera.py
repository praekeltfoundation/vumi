from django.test import TestCase
from django.core.urlresolvers import reverse
from django.contrib.auth.models import User

from vumi.webapp.api.models import SentSMS, ReceivedSMS
from vumi.webapp.api.tests.utils import APIClient, mock_sent_messages


class OperaSMSHandlerTestCase(TestCase):

    fixtures = ['user_set']

    def setUp(self):
        self.client = APIClient()
        self.client.login(username='api', password='password')
        # create the user we need to be authorized
        self.user = User.objects.get(username='api')

    def test_sms_receipts(self):
        """
        Receipts received from opera should update the status
        """
        [sms] = mock_sent_messages(self.user, count=1,
                                    transport_name="Opera",
                                    transport_msg_id="001efc31")
        self.assertEquals(sms.transport_status, '')

        raw_xml_post = """
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
        """

        resp = self.client.post(reverse('api:opera:sms-receipt'),
                                raw_xml_post.strip(), content_type='text/xml')
        sms = SentSMS.objects.get(pk=sms.pk)  # reload
        self.assertEquals(sms.transport_status, 'D')
        self.assertEquals(resp.status_code, 201)

    def test_sms_receiving_with_text_plain_headers(self):
        """
        By eavesdropping we got the following log, this is what opera sends.
        DTD is available at https://dragon.sa.operatelecom.com/MEnable/Client/
        Extra/bspostevent-1_0_0.dtd

        POST /api/v1/sms/opera/receive.xml HTTP/1.1
        Content-Length: 1798
        Content-Type: text/plain; charset=utf-8
        Authorization: ...
        Host: ....
        """
        receive_sms_doc = """
        <?xml version="1.0"?>
        <!DOCTYPE bspostevent>
        <bspostevent>
          <field name="MOReference" type = "string">282341913</field>
          <field name="IsReceipt" type = "string">NO</field>
          <field name="RemoteNetwork" type = "string">mtn-za</field>
          <field name="BSDate-tomorrow" type = "string">20100605</field>
          <field name="BSDate-today" type = "string">20100604</field>
          <field name="ReceiveDate"
                 type = "date">2010-06-04 15:51:25 +0000</field>
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
          <field name="ServiceEndDate"
                 type = "string">2010-06-30 07:47:00 +0200</field>
          <field name="Now" type = "date">2010-06-04 15:51:27 +0000</field>
        </bspostevent>
        """

        self.assertEquals(ReceivedSMS.objects.count(), 0)
        resp = self.client.post(reverse('api:opera:sms-receive'),
                                    receive_sms_doc.strip(),
                                    content_type='text/plain; charset=utf-8')
        self.assertEquals(resp.status_code, 200)
        self.assertEquals(ReceivedSMS.objects.count(), 1)
        sms = ReceivedSMS.objects.latest()
        self.assertEquals(
            sms.received_at.strftime('%Y-%m-%d %H:%M:%S +0000'),
            '2010-06-04 15:51:25 +0000',
        )
        self.assertEquals(sms.from_msisdn, '+27831234567')
        self.assertEquals(sms.to_msisdn, '*32323')
        self.assertEquals(sms.transport_name, 'Opera')
