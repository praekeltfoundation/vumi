from twisted.trial.unittest import TestCase

from vumi.message import (TransportMessage, TransportSMS, TransportSMSAck,
                          TransportSMSDeliveryReport)


class MessageTest(TestCase):
    def test_basic_transport_message(self):
        msg = TransportMessage(
            message_type='sms',
            message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            message='heya',
            transport='sphex',
            )
        self.assertEqual('sms', msg['message_type'])
        self.assertEqual('abc', msg['message_id'])
        self.assertEqual('20110907', msg['message_version'])
        self.assertEqual('heya', msg['message'])
        self.assertEqual('sphex', msg['transport'])

    def test_basic_transport_sms(self):
        msg = TransportSMS(
            message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            message='heya',
            transport='sphex',
            )
        self.assertEqual('sms', msg['message_type'])
        self.assertEqual('abc', msg['message_id'])
        self.assertEqual('20110907', msg['message_version'])
        self.assertEqual('heya', msg['message'])
        self.assertEqual('sphex', msg['transport'])

    def test_basic_transport_sms_ack(self):
        msg = TransportSMSAck(
            message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            transport='sphex',
            transport_message_id='def',
            )
        self.assertEqual('sms_ack', msg['message_type'])
        self.assertEqual('abc', msg['message_id'])
        self.assertEqual('20110907', msg['message_version'])
        self.assertEqual('sphex', msg['transport'])
        self.assertEqual('def', msg['transport_message_id'])

    def test_basic_transport_sms_delivery_report(self):
        msg = TransportSMSDeliveryReport(
            message_id='abc',
            to_addr='+27831234567',
            from_addr='12345',
            transport='sphex',
            transport_message_id='def',
            delivery_status='delivered',
            )
        self.assertEqual('sms_delivery_report', msg['message_type'])
        self.assertEqual('abc', msg['message_id'])
        self.assertEqual('20110907', msg['message_version'])
        self.assertEqual('sphex', msg['transport'])
        self.assertEqual('def', msg['transport_message_id'])
        self.assertEqual('delivered', msg['delivery_status'])
