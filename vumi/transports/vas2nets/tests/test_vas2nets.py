# encoding: utf-8
import string
from uuid import uuid4
from datetime import datetime

from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from twisted.trial import unittest
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.web.test.test_web import DummyRequest

from vumi.tests.utils import (get_stubbed_worker, TestResourceWorker,
                              UTCNearNow, RegexMatcher)
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.message import from_json, TransportUserMessage, TransportEvent
from vumi.transports.vas2nets.vas2nets import (
    ReceiveSMSResource, DeliveryReceiptResource, Vas2NetsTransport,
    validate_characters, Vas2NetsEncodingError, normalize_outbound_msisdn)


class TestResource(Resource):
    isLeaf = True

    def __init__(self, message_id, message, code=http.OK, send_id=None):
        self.message_id = message_id
        self.message = message
        self.code = code
        self.send_id = send_id

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

        if self.send_id is not None:
            assert request.args['messageid'] == [self.send_id]

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
        w = get_stubbed_worker(Vas2NetsTransport, self.config, self.broker)
        w.transport_name = 'vas2nets'
        w.event_publisher = yield w.publish_rkey('events')
        w.message_publisher = yield w.publish_rkey('inbound')
        self.workers = [w]
        self.worker = w
        self.today = datetime.utcnow().date()

    def tearDown(self):
        for worker in self.workers:
            worker.stopWorker()

    def make_resource_worker(self, msg_id, msg, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
                (self.path, TestResource, (msg_id, msg, code, send_id))])
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
            'messageid': [str(uuid4())],
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

    def make_ack(self, sent_message_id):
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='ack',
            user_message_id='1',
            sent_message_id=sent_message_id,
            timestamp=UTCNearNow(),
            transport_name='vas2nets',
            )

    def make_delivery_message(self, status, tr_status, tr_message):
        return TransportEvent(
            event_id=RegexMatcher(r'^[0-9a-fA-F]{32}$'),
            event_type='delivery_report',
            transport_name='vas2nets',
            user_message_id='vas2nets.abc',
            delivery_status=status,
            transport_metadata={
               'delivery_status': tr_status,
               'network_id': 'provider',
               'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
               'delivery_message': tr_message,
               },
            to_addr='+41791234567',
            timestamp=UTCNearNow(),
            )

    def mkmsg_in(self):
        msg = TransportUserMessage(
            from_addr='+41791234567',
            to_addr='9292',
            message_id='vas2nets.abc',
            transport_name='vas2nets',
            transport_type='sms',
            transport_metadata={
                'original_message_id': 'vas2nets.abc',
                'network_id': 'provider',
                'keyword': '',
                'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
                },
            content='hello world',
            timestamp=UTCNearNow(),
            )
        return msg

    def mkmsg_out(self, in_reply_to=None):
        msg = TransportUserMessage(
            transport_name='vas2nets',
            transport_type='sms',
            to_addr='+27761234567',
            from_addr='9292',
            message_id='1',
            in_reply_to=in_reply_to,
            transport_metadata={
                'network_id': 'network-id',
                },
            content='hello world',
            )
        return msg

    @inlineCallbacks
    def test_receive_sms(self):
        resource = ReceiveSMSResource(self.config, self.worker.publish_message)
        request = self.create_request({
            'messageid': ['abc'],
            'text': ['hello world'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEqual(response, NOT_DONE_YET)
        yield d
        self.assertEqual('', ''.join(request.written))
        self.assertEqual(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEqual(request.responseCode, http.OK)
        msg = self.mkmsg_in()

        [smsg] = self.get_dispatched('vas2nets.inbound')
        self.assertEqual(msg.payload, from_json(smsg.body))

    @inlineCallbacks
    def test_delivery_receipt_pending(self):
        resource = DeliveryReceiptResource(self.config,
                                           self.worker.publish_delivery_report)

        request = self.create_request({
            'smsid': ['1'],
            'messageid': ['abc'],
            'sender': ['+41791234567'],
            'status': ['1'],
            'text': ['Message submitted to Provider for delivery.'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEqual(response, NOT_DONE_YET)
        yield d
        self.assertEqual('', ''.join(request.written))
        self.assertEqual(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEqual(request.responseCode, http.OK)
        msg = self.make_delivery_message(
            'pending', '1', 'Message submitted to Provider for delivery.')
        [smsg] = self.get_dispatched('vas2nets.events')
        self.assertEqual(msg, from_json(smsg.body))

    @inlineCallbacks
    def test_delivery_receipt_failed(self):
        resource = DeliveryReceiptResource(self.config,
                                           self.worker.publish_delivery_report)

        request = self.create_request({
            'smsid': ['1'],
            'messageid': ['abc'],
            'sender': ['+41791234567'],
            'status': ['-9'],
            'text': ['Message could not be delivered.'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEqual(response, NOT_DONE_YET)
        yield d
        self.assertEqual('', ''.join(request.written))
        self.assertEqual(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEqual(request.responseCode, http.OK)
        msg = self.make_delivery_message(
            'failed', '-9', 'Message could not be delivered.')
        [smsg] = self.get_dispatched('vas2nets.events')
        self.assertEqual(from_json(smsg.body), msg)

    @inlineCallbacks
    def test_delivery_receipt_delivered(self):
        resource = DeliveryReceiptResource(self.config,
                                           self.worker.publish_delivery_report)

        request = self.create_request({
            'smsid': ['1'],
            'messageid': ['abc'],
            'sender': ['+41791234567'],
            'status': ['2'],
            'text': ['Message delivered to MSISDN.'],
        })
        d = request.notifyFinish()
        response = resource.render(request)
        self.assertEqual(response, NOT_DONE_YET)
        yield d
        self.assertEqual('', ''.join(request.written))
        self.assertEqual(request.outgoingHeaders['content-type'],
                          'text/plain')
        self.assertEqual(request.responseCode, http.OK)
        msg = self.make_delivery_message(
            'delivered', '2', 'Message delivered to MSISDN.')
        [smsg] = self.get_dispatched('vas2nets.events')
        self.assertEqual(from_json(smsg.body), msg)

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
        mocked_message_id = str(uuid4())
        mocked_message = "Result_code: 00, Message OK"

        # open an HTTP resource that mocks the Vas2Nets response for the
        # duration of this test
        yield self.make_resource_worker(mocked_message_id, mocked_message)
        yield self.worker.startWorker()

        yield self.worker._process_message(self.mkmsg_out())

        [smsg] = self.get_dispatched('vas2nets.events')
        self.assertEqual(self.make_ack(mocked_message_id),
                         from_json(smsg.body))

    @inlineCallbacks
    def test_send_sms_reply_success(self):
        mocked_message_id = str(uuid4())
        reply_to_msgid = str(uuid4())
        mocked_message = "Result_code: 00, Message OK"

        # open an HTTP resource that mocks the Vas2Nets response for the
        # duration of this test
        yield self.make_resource_worker(mocked_message_id, mocked_message,
                                        send_id=reply_to_msgid)
        yield self.worker.startWorker()

        yield self.worker._process_message(self.mkmsg_out(
                    in_reply_to=reply_to_msgid))

        [smsg] = self.get_dispatched('vas2nets.events')
        self.assertEqual(self.make_ack(mocked_message_id),
                         from_json(smsg.body))

    @inlineCallbacks
    def test_send_sms_fail(self):
        mocked_message_id = False
        mocked_message = ("Result_code: 04, Internal system error occurred "
                          "while processing message")
        yield self.make_resource_worker(mocked_message_id, mocked_message)
        yield self.worker.startWorker()

        msg = self.mkmsg_out()
        d = self.worker._process_message(msg)
        yield d
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = from_json(fmsg.body)
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertTrue(
            "Vas2NetsTransportError: No SmsId Header" in fmsg['reason'])

    @inlineCallbacks
    def test_send_sms_noconn(self):
        yield self.worker.startWorker()

        msg = self.mkmsg_out()
        d = self.worker._process_message(msg)
        yield d
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = from_json(fmsg.body)
        self.assertEqual(msg, fmsg['message'])
        self.assertEqual("connection refused", fmsg['reason'])

    @inlineCallbacks
    def test_send_sms_not_OK(self):
        mocked_message = "Page not found."
        yield self.make_resource_worker(None, mocked_message, http.NOT_FOUND)
        yield self.worker.startWorker()

        msg = self.mkmsg_out()
        deferred = self.worker._process_message(msg)
        yield deferred
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = from_json(fmsg.body)
        self.assertEqual(msg, fmsg['message'])
        self.assertTrue(fmsg['reason'].startswith("server error"))

    def test_normalize_outbound_msisdn(self):
        self.assertEqual(normalize_outbound_msisdn('+27761234567'),
                          '0027761234567')
