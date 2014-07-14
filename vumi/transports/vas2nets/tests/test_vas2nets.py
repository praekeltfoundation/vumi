# encoding: utf-8
import string
from datetime import datetime
from urllib import urlencode

from twisted.web import http
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request_full
from vumi.message import TransportMessage
from vumi.transports.failures import TemporaryFailure, PermanentFailure
from vumi.transports.base import FailureMessage
from vumi.transports.vas2nets.vas2nets import (
    Vas2NetsTransport, validate_characters, Vas2NetsTransportError,
    Vas2NetsEncodingError, normalize_outbound_msisdn)
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import MockHttpServer
from vumi.transports.tests.helpers import TransportHelper


class TestVas2NetsTransport(VumiTestCase):

    transport_type = 'sms'

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'url': None,
            'username': 'username',
            'password': 'password',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice',
            'web_receive_path': '/receive',
            'web_receipt_path': '/receipt',
            'web_port': 0,
        }
        self.tx_helper = self.add_helper(
            TransportHelper(Vas2NetsTransport, transport_name='vas2nets'))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()
        self.today = datetime.utcnow().date()

    def _make_handler(self, message_id, message, code, send_id):
        def handler(request):
            log.msg(request.content.read())
            request.setResponseCode(code)
            required_fields = [
                'username', 'password', 'call-number', 'origin', 'text',
                'messageid', 'provider', 'tariff', 'owner', 'service',
                'subservice'
            ]
            log.msg('request.args', request.args)
            for key in required_fields:
                log.msg('checking for %s' % key)
                self.assertTrue(key in request.args)

            if send_id is not None:
                self.assertEqual(request.args['messageid'], [send_id])

            if message_id:
                request.setHeader('X-Nth-Smsid', message_id)
            return message
        return handler

    @inlineCallbacks
    def start_mock_server(self, msg_id, msg, code=http.OK, send_id=None):
        self.mock_server = MockHttpServer(
            self._make_handler(msg_id, msg, code, send_id))
        self.add_cleanup(self.mock_server.stop)
        yield self.mock_server.start()
        self.transport.config['url'] = self.mock_server.url

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
        url = self.transport_url + path
        return http_request_full(url, urlencode(args), {
                'Content-Type': ['application/x-www-form-urlencoded'],
                })

    def make_delivery_report(self, status, tr_status, tr_message):
        transport_metadata = {
            'delivery_message': tr_message,
            'delivery_status': tr_status,
            'network_id': 'provider',
            'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            }
        return self.tx_helper.make_delivery_report(
            self.tx_helper.make_outbound("foo", message_id="vas2nets.abc"),
            to_addr="+41791234567", delivery_status=status,
            transport_metadata=transport_metadata)

    def make_dispatch_outbound(self, content, **kw):
        transport_metadata = {
            'original_message_id': 'vas2nets.def',
            'keyword': '',
            'network_id': 'provider',
            'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            }
        kw.setdefault('transport_metadata', transport_metadata)
        return self.tx_helper.make_dispatch_outbound(content, **kw)

    def assert_events_equal(self, expected, received):
        to_payload = lambda m: dict(
            (k, v) for k, v in m.payload.iteritems()
            if k not in ('event_id', 'timestamp', 'transport_type'))
        self.assertEqual(to_payload(expected), to_payload(received))

    def assert_messages_equal(self, expected, received):
        to_payload = lambda m: dict(
            (k, v) for k, v in m.payload.iteritems()
            if k not in ('message_id', 'timestamp'))
        self.assertEqual(to_payload(expected), to_payload(received))

    @inlineCallbacks
    def test_health_check(self):
        response = yield http_request_full(self.transport_url + "/health")

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
        [msg] = self.tx_helper.get_dispatched_inbound()
        expected_msg = self.tx_helper.make_inbound(
            "hello world", message_id='vas2nets.abc',
            transport_metadata={
                'original_message_id': 'vas2nets.abc',
                'keyword': '',
                'network_id': 'provider',
                'timestamp': self.today.strftime('%Y-%m-%dT%H:%M:%S'),
            })
        self.assert_messages_equal(expected_msg, msg)

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
        msg = self.make_delivery_report(
            'pending', '1', 'Message submitted to Provider for delivery.')
        [dr] = self.tx_helper.get_dispatched_events()
        self.assert_events_equal(msg, dr)

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
        msg = self.make_delivery_report(
            'failed', '-9', 'Message could not be delivered.')
        [dr] = self.tx_helper.get_dispatched_events()
        self.assert_events_equal(msg, dr)

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
        msg = self.make_delivery_report(
            'delivered', '2', 'Message delivered to MSISDN.')
        [dr] = self.tx_helper.get_dispatched_events()
        self.assert_events_equal(msg, dr)

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
        yield self.start_mock_server(mocked_message_id, mocked_message)

        sent_msg = yield self.make_dispatch_outbound("hello")

        msg = self.tx_helper.make_ack(
            sent_msg, sent_message_id=mocked_message_id)
        [ack] = self.tx_helper.get_dispatched_events()
        self.assert_events_equal(msg, ack)

    @inlineCallbacks
    def test_send_sms_reply_success(self):
        mocked_message_id = TransportMessage.generate_id()
        reply_to_msgid = TransportMessage.generate_id()
        mocked_message = "Result_code: 00, Message OK"

        # open an HTTP resource that mocks the Vas2Nets response for the
        # duration of this test
        yield self.start_mock_server(mocked_message_id, mocked_message,
                                        send_id=reply_to_msgid)

        sent_msg = yield self.make_dispatch_outbound(
            "hello", in_reply_to=reply_to_msgid)

        msg = self.tx_helper.make_ack(
            sent_msg, sent_message_id=mocked_message_id)
        [ack] = self.tx_helper.get_dispatched_events()
        self.assert_events_equal(msg, ack)

    @inlineCallbacks
    def test_send_sms_fail(self):
        mocked_message_id = False
        mocked_message = ("Result_code: 04, Internal system error occurred "
                          "while processing message")
        yield self.start_mock_server(mocked_message_id, mocked_message)

        msg = yield self.make_dispatch_outbound("hello")

        [twisted_failure] = self.flushLoggedErrors(Vas2NetsTransportError)
        failure = twisted_failure.value
        self.assertTrue("No SmsId Header" in str(failure))

        [fmsg] = self.tx_helper.get_dispatched_failures()
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertTrue(
            "Vas2NetsTransportError: No SmsId Header" in fmsg['reason'])

        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertTrue("No SmsId Header" in nack['nack_reason'])

    @inlineCallbacks
    def test_send_sms_noconn(self):
        # TODO: Figure out a solution that doesn't require hoping that
        #       nothing's listening on this port.
        self.transport.config['url'] = 'http://127.0.0.1:9999/'
        msg = yield self.make_dispatch_outbound("hello")

        [twisted_failure] = self.flushLoggedErrors(TemporaryFailure)
        failure = twisted_failure.value
        self.assertTrue("connection refused" in str(failure))

        [fmsg] = self.tx_helper.get_dispatched_failures()
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertEqual(fmsg['failure_code'],
                         FailureMessage.FC_TEMPORARY)
        self.assertTrue(fmsg['reason'].strip().endswith("connection refused"))

    @inlineCallbacks
    def test_send_sms_not_OK(self):
        mocked_message = "Page not found."
        yield self.start_mock_server(None, mocked_message, http.NOT_FOUND)

        msg = yield self.make_dispatch_outbound("hello")

        [twisted_failure] = self.flushLoggedErrors(PermanentFailure)
        failure = twisted_failure.value
        self.assertTrue("server error: HTTP 404:" in str(failure))

        [fmsg] = self.tx_helper.get_dispatched_failures()
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertEqual(fmsg['failure_code'],
                         FailureMessage.FC_PERMANENT)
        self.assertTrue(fmsg['reason'].strip()
                        .endswith("server error: HTTP 404: Page not found."))

    def test_normalize_outbound_msisdn(self):
        self.assertEqual(normalize_outbound_msisdn('+27761234567'),
                          '0027761234567')
