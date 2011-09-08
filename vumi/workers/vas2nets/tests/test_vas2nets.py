# encoding: utf-8
import string
from uuid import uuid1
from datetime import datetime
import json

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.trial import unittest
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.web.test.test_web import DummyRequest

from vumi.tests.utils import get_stubbed_worker, TestResourceWorker, UTCNearNow
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.message import (from_json, Message, TransportSMS, TransportSMSAck,
                          TransportSMSDeliveryReport)
from vumi.workers.vas2nets.transport import (
    ReceiveSMSResource, DeliveryReceiptResource, Vas2NetsTransport,
    validate_characters, Vas2NetsEncodingError, normalize_outbound_msisdn)


class TestResource(Resource):
    isLeaf = True

    def __init__(self, message_id, message, code=http.OK):
        self.message_id = message_id
        self.message = message
        self.code = code

    def render_POST(self, request):
        log.msg(request.content.read())
        request.setResponseCode(self.code)
        required_fields = [
            'username', 'password', 'call-number', 'origin', 'text',
            'messageid', 'provider', 'tariff', 'owner', 'service',
            'subservice'
        ]
        log.msg('request.args', request.args)
        for key in required_fields:
            log.msg('checking for %s' % key)
            assert key in request.args

        if self.message_id:
            request.setHeader('X-Nth-Smsid', self.message_id)
        return self.message


class Vas2NetsTransportTestCase(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        self.path = '/api/v1/sms/vas2nets/receive/'
        self.port = 9999
        self.config = {
            'transport_name': 'vas2nets',
            'url': 'http://localhost:%s%s' % (self.port, self.path),
            'username': 'username',
            'password': 'password',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice',
            'web_receive_path': '/receive',
            'web_receipt_path': '/receipt',
            'web_port': 9998,
        }
        self.broker = FakeAMQPBroker()
        self.worker = get_stubbed_worker(Vas2NetsTransport, self.config,
                                         self.broker)
        self.publisher = yield self.worker.publish_to('some.routing.key')
        self.today = datetime.utcnow().date()
        self.workers = [self.worker]

    def tearDown(self):
        for worker in self.workers:
            worker.stopWorker()

    def make_resource_worker(self, msg_id, msg, code=http.OK):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([(self.path, TestResource, (msg_id, msg, code))])
        self.workers.append(w)
        return w.startWorker()

    def get_dispatched(self, rkey):
        return self.broker.get_dispatched('vumi', rkey)

    def create_request(self, dictionary={}, path='/', method='POST'):
        """
        Creates a dummy Vas2Nets request for testing our resources with
        """
        request = DummyRequest(path)
        request.method = method
        args = {
            'messageid': [str(uuid1())],
            'time': [self.today.strftime('%Y.%m.%d %H:%M:%S')],
            'sender': ['0041791234567'],
            'destination': ['9292'],
            'provider': ['provider'],
            'keyword': [''],
            'header': [''],
            'text': [''],
            'keyword': [''],
        }
        args.update(dictionary)
        request.args = args
        return request

    def make_delivery_message(self, status, tr_status, tr_message):
        return TransportSMSDeliveryReport(
            transport='vas2nets',
            transport_message_id='1',
            transport_metadata={
               'delivery_status': tr_status,
               'network_id': 'provider',
               'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
               'delivery_message': tr_message,
               },
            to_addr='+41791234567',
            message_id='internal id',
            delivery_status=status,
            timestamp=UTCNearNow(),
            )

    def mkmsg_in(self):
        msg = TransportSMS(
            message_type='sms',
            message_version='20110907',
            from_addr='+41791234567',
            to_addr='9292',
            message_id='1',
            transport='vas2nets',
            transport_message_id='1',
            transport_metadata={
                'network_id': 'provider',
                'keyword': '',
                'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
                },
            message='hello world',
            timestamp=UTCNearNow(),
            )
        return msg

    def mkmsg_out(self, **fields):
        msg = {
            'to_msisdn': '+27761234567',
            'from_msisdn': '9292',
            'id': '1',
            'reply_to': '',
            'transport_network_id': 'network-id',
            'message': 'hello world',
            }
        msg.update(fields)
        return msg

    @inlineCallbacks
    def test_receive_sms(self):
        resource = ReceiveSMSResource(self.config, self.publisher)
        request = self.create_request({
            'messageid': ['1'],
            'text': ['hello world'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEquals(response, NOT_DONE_YET)
        yield d
        self.assertEquals('', ''.join(request.written))
        self.assertEquals(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        msg = self.mkmsg_in()

        [smsg] = self.get_dispatched('sms.inbound.vas2nets.9292')
        self.assertEquals(msg.payload, from_json(smsg.body))

    @inlineCallbacks
    def test_delivery_receipt_pending(self):
        resource = DeliveryReceiptResource(self.config, self.publisher)

        request = self.create_request({
            'smsid': ['1'],
            'messageid': ['internal id'],
            'sender': ['+41791234567'],
            'status': ['1'],
            'text': ['Message submitted to Provider for delivery.'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEquals(response, NOT_DONE_YET)
        yield d
        self.assertEquals('', ''.join(request.written))
        self.assertEquals(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        msg = self.make_delivery_message(
            'pending', '1', 'Message submitted to Provider for delivery.')
        [smsg] = self.get_dispatched('sms.receipt.vas2nets')
        self.assertEquals(Message.from_json(smsg.body), msg)

    @inlineCallbacks
    def test_delivery_receipt_failed(self):
        resource = DeliveryReceiptResource(self.config, self.publisher)

        request = self.create_request({
            'smsid': ['1'],
            'messageid': ['internal id'],
            'sender': ['+41791234567'],
            'status': ['-9'],
            'text': ['Message could not be delivered.'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEquals(response, NOT_DONE_YET)
        yield d
        self.assertEquals('', ''.join(request.written))
        self.assertEquals(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        msg = self.make_delivery_message(
            'failed', '-9', 'Message could not be delivered.')
        [smsg] = self.get_dispatched('sms.receipt.vas2nets')
        self.assertEquals(Message.from_json(smsg.body), msg)

    @inlineCallbacks
    def test_delivery_receipt_delivered(self):
        resource = DeliveryReceiptResource(self.config, self.publisher)

        request = self.create_request({
            'smsid': ['1'],
            'messageid': ['internal id'],
            'sender': ['+41791234567'],
            'status': ['2'],
            'text': ['Message delivered to MSISDN.'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEquals(response, NOT_DONE_YET)
        yield d
        self.assertEquals('', ''.join(request.written))
        self.assertEquals(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEquals(request.responseCode, http.OK)
        msg = self.make_delivery_message(
            'delivered', '2', 'Message delivered to MSISDN.')
        [smsg] = self.get_dispatched('sms.receipt.vas2nets')
        self.assertEquals(Message.from_json(smsg.body), msg)

    def test_validate_characters(self):
        self.assertRaises(Vas2NetsEncodingError, validate_characters,
                            u"ïøéå¬∆˚")
        self.assertTrue(validate_characters(string.ascii_lowercase))
        self.assertTrue(validate_characters(string.ascii_uppercase))
        self.assertTrue(validate_characters('0123456789'))
        self.assertTrue(validate_characters(u'äöü ÄÖÜ àùò ìèé §Ññ £$@'))
        self.assertTrue(validate_characters(u'/?!#%&()*+,-:;<=>.'))
        self.assertTrue(validate_characters(u'testing\ncarriage\rreturns'))
        self.assertTrue(validate_characters(u'testing "quotes"'))
        self.assertTrue(validate_characters(u"testing 'quotes'"))

    @inlineCallbacks
    def test_send_sms_success(self):
        mocked_message_id = str(uuid1())
        mocked_message = "Result_code: 00, Message OK"

        # open an HTTP resource that mocks the Vas2Nets response for the
        # duration of this test
        yield self.make_resource_worker(mocked_message_id, mocked_message)
        yield self.worker.startWorker()

        yield self.worker.handle_outbound_message(Message(**self.mkmsg_out()))

        [smsg] = self.get_dispatched('sms.ack.vas2nets')
        self.assertEqual(TransportSMSAck(
                message_id='1',
                transport_message_id=mocked_message_id,
                timestamp=UTCNearNow(),
                ), from_json(smsg.body))

    @inlineCallbacks
    def test_send_sms_fail(self):
        mocked_message_id = False
        mocked_message = "Result_code: 04, Internal system error occurred " \
                            "while processing message"
        yield self.make_resource_worker(mocked_message_id, mocked_message)
        yield self.worker.startWorker()

        msg = self.mkmsg_out()
        deferred = self.worker.handle_outbound_message(Message(**msg))
        yield deferred
        [fmsg] = self.get_dispatched('sms.outbound.vas2nets.failures')
        fmsg = json.loads(fmsg.body)
        self.assertEqual(msg, fmsg['message'])
        self.assertTrue(
            "Vas2NetsTransportError: No SmsId Header" in fmsg['reason'])

    @inlineCallbacks
    def test_send_sms_noconn(self):
        yield self.worker.startWorker()

        msg = self.mkmsg_out()
        deferred = self.worker.handle_outbound_message(Message(**msg))
        yield deferred
        [fmsg] = self.get_dispatched('sms.outbound.vas2nets.failures')
        fmsg = json.loads(fmsg.body)
        self.assertEqual(msg, fmsg['message'])
        self.assertEqual("connection refused", fmsg['reason'])

    @inlineCallbacks
    def test_send_sms_not_OK(self):
        mocked_message = "Page not found."
        yield self.make_resource_worker(None, mocked_message, http.NOT_FOUND)
        yield self.worker.startWorker()

        msg = self.mkmsg_out()
        deferred = self.worker.handle_outbound_message(Message(**msg))
        yield deferred
        [fmsg] = self.get_dispatched('sms.outbound.vas2nets.failures')
        fmsg = json.loads(fmsg.body)
        self.assertEqual(msg, fmsg['message'])
        self.assertTrue(fmsg['reason'].startswith("server error"))

    def test_normalize_outbound_msisdn(self):
        self.assertEquals(normalize_outbound_msisdn('+27761234567'),
                          '0027761234567')
