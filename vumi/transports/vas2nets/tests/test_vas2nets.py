# encoding: utf-8
import string
from datetime import datetime
from urllib import urlencode

from twisted.web import http
from twisted.web.resource import Resource
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi.transports.tests.test_base import TransportTestCase
from vumi.tests.utils import get_stubbed_worker, TestResourceWorker
from vumi.message import TransportMessage
from vumi.transports.base import FailureMessage
from vumi.transports.vas2nets.vas2nets import (
    Vas2NetsTransport, validate_characters, Vas2NetsEncodingError,
    normalize_outbound_msisdn)


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


class Vas2NetsTransportTestCase(TransportTestCase):

    transport_name = 'vas2nets'
    transport_type = 'sms'
    transport_class = Vas2NetsTransport

    @inlineCallbacks
    def setUp(self):
        yield super(Vas2NetsTransportTestCase, self).setUp()
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
        self.worker = yield self.get_transport(self.config)
        self.today = datetime.utcnow().date()

    def make_resource_worker(self, msg_id, msg, code=http.OK, send_id=None):
        w = get_stubbed_worker(TestResourceWorker, {})
        w.set_resources([
                (self.path, TestResource, (msg_id, msg, code, send_id))])
        self._workers.append(w)
        return w.startWorker()

    def get_dispatched(self, rkey):
        return self._amqp.get_dispatched('vumi', rkey)

    def make_request(self, path, qparams):
        """
        Builds a request URL with the appropriate params.
        """
        args = {
            'messageid': TransportMessage.generate_id(),
            'time': self.today.strftime('%Y.%m.%d %H:%M:%S'),
            'sender': '0041791234567',
            'destination': '9292',
            'provider': 'provider',
            'keyword': '',
            'header': '',
            'text': '',
            'keyword': '',
        }
        args.update(qparams)
        url = "http://localhost:%s/%s" % (
            self.config['web_port'], path.lstrip('/'))
        return http_request_full(url, urlencode(args), {
                'Content-Type': ['application/x-www-form-urlencoded'],
                })

    def mkmsg_delivery(self, status, tr_status, tr_message):
        transport_metadata = {
            'delivery_message': tr_message,
            'delivery_status': tr_status,
            'network_id': 'provider',
            'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            }
        return super(Vas2NetsTransportTestCase, self).mkmsg_delivery(
            status, user_message_id='vas2nets.abc',
            transport_metadata=transport_metadata)

    def mkmsg_in(self):
        transport_metadata = {
            'original_message_id': 'vas2nets.abc',
            'keyword': '',
            'network_id': 'provider',
            'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            }
        return super(Vas2NetsTransportTestCase, self).mkmsg_in(
            message_id='vas2nets.abc', transport_metadata=transport_metadata)

    def mkmsg_out(self, **kw):
        transport_metadata = {
            'original_message_id': 'vas2nets.def',
            'keyword': '',
            'network_id': 'provider',
            'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            }
        kw.setdefault('transport_metadata', transport_metadata)
        # kw.setdefault('message_id', 'vas2nets.abc')
        kw.setdefault('stubs', False)
        return super(Vas2NetsTransportTestCase, self).mkmsg_out(**kw)

    @inlineCallbacks
    def test_health_check(self):
        url = "http://localhost:%s/health" % (self.config['web_port'],)
        response = yield http_request_full(url)

        self.assertEqual('OK', response.delivered_body)
        self.assertEqual(response.code, http.OK)

    @inlineCallbacks
    def test_receive_sms(self):
        response = yield self.make_request('/receive', {
                    'messageid': 'abc',
                    'text': 'hello world',
                    })

        self.assertEqual('', response.delivered_body)
        self.assertEqual(response.headers.getRawHeaders('content-type'),
                         ['text/plain'])
        self.assertEqual(response.code, http.OK)
        msg = self.mkmsg_in()

        [smsg] = self.get_dispatched('vas2nets.inbound')
        self.assertEqual(msg, TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_delivery_receipt_pending(self):
        response = yield self.make_request('/receipt', {
            'smsid': '1',
            'messageid': 'abc',
            'sender': '+41791234567',
            'status': '1',
            'text': 'Message submitted to Provider for delivery.',
        })
        self.assertEqual('', response.delivered_body)
        self.assertEqual(response.headers.getRawHeaders('content-type'),
                         ['text/plain'])
        self.assertEqual(response.code, http.OK)
        msg = self.mkmsg_delivery(
            'pending', '1', 'Message submitted to Provider for delivery.')
        [smsg] = self.get_dispatched('vas2nets.event')
        self.assertEqual(msg, TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_delivery_receipt_failed(self):
        response = yield self.make_request('/receipt', {
            'smsid': '1',
            'messageid': 'abc',
            'sender': '+41791234567',
            'status': '-9',
            'text': 'Message could not be delivered.',
        })
        self.assertEqual('', response.delivered_body)
        self.assertEqual(response.headers.getRawHeaders('content-type'),
                         ['text/plain'])
        self.assertEqual(response.code, http.OK)
        msg = self.mkmsg_delivery(
            'failed', '-9', 'Message could not be delivered.')
        [smsg] = self.get_dispatched('vas2nets.event')
        self.assertEqual(msg, TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_delivery_receipt_delivered(self):
        response = yield self.make_request('/receipt', {
            'smsid': '1',
            'messageid': 'abc',
            'sender': '+41791234567',
            'status': '2',
            'text': 'Message delivered to MSISDN.',
        })
        self.assertEqual('', response.delivered_body)
        self.assertEqual(response.headers.getRawHeaders('content-type'),
                         ['text/plain'])
        self.assertEqual(response.code, http.OK)
        msg = self.mkmsg_delivery(
            'delivered', '2', 'Message delivered to MSISDN.')
        [smsg] = self.get_dispatched('vas2nets.event')
        self.assertEqual(msg, TransportMessage.from_json(smsg.body))

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
        mocked_message_id = TransportMessage.generate_id()
        mocked_message = "Result_code: 00, Message OK"

        # open an HTTP resource that mocks the Vas2Nets response for the
        # duration of this test
        yield self.make_resource_worker(mocked_message_id, mocked_message)

        yield self.dispatch(self.mkmsg_out())

        [smsg] = self.get_dispatched('vas2nets.event')
        self.assertEqual(self.mkmsg_ack(sent_message_id=mocked_message_id),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_send_sms_reply_success(self):
        mocked_message_id = TransportMessage.generate_id()
        reply_to_msgid = TransportMessage.generate_id()
        mocked_message = "Result_code: 00, Message OK"

        # open an HTTP resource that mocks the Vas2Nets response for the
        # duration of this test
        yield self.make_resource_worker(mocked_message_id, mocked_message,
                                        send_id=reply_to_msgid)

        yield self.dispatch(self.mkmsg_out(in_reply_to=reply_to_msgid))

        [smsg] = self.get_dispatched('vas2nets.event')
        self.assertEqual(self.mkmsg_ack(sent_message_id=mocked_message_id),
                         TransportMessage.from_json(smsg.body))

    @inlineCallbacks
    def test_send_sms_fail(self):
        mocked_message_id = False
        mocked_message = ("Result_code: 04, Internal system error occurred "
                          "while processing message")
        yield self.make_resource_worker(mocked_message_id, mocked_message)

        msg = self.mkmsg_out()
        d = self.dispatch(msg)
        yield d
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = TransportMessage.from_json(fmsg.body)
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertTrue(
            "Vas2NetsTransportError: No SmsId Header" in fmsg['reason'])

    @inlineCallbacks
    def test_send_sms_noconn(self):
        msg = self.mkmsg_out()
        d = self.dispatch(msg)
        yield d
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = TransportMessage.from_json(fmsg.body)
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertEqual(fmsg['failure_code'],
                         FailureMessage.FC_TEMPORARY)
        self.assertTrue(fmsg['reason'].strip().endswith("connection refused"))

    @inlineCallbacks
    def test_send_sms_not_OK(self):
        mocked_message = "Page not found."
        yield self.make_resource_worker(None, mocked_message, http.NOT_FOUND)

        msg = self.mkmsg_out()
        deferred = self.dispatch(msg)
        yield deferred
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = TransportMessage.from_json(fmsg.body)
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertEqual(fmsg['failure_code'],
                         FailureMessage.FC_PERMANENT)
        self.assertTrue(fmsg['reason'].strip()
                        .endswith("server error: HTTP 404: Page not found."))

    def test_normalize_outbound_msisdn(self):
        self.assertEqual(normalize_outbound_msisdn('+27761234567'),
                          '0027761234567')
