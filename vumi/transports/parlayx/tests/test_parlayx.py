from functools import partial

from twisted.internet.defer import inlineCallbacks, succeed, fail
from twisted.trial.unittest import TestCase

from vumi.transports.failures import PermanentFailure
from vumi.transports.parlayx import ParlayXTransport
from vumi.transports.parlayx.parlayx import (
    unique_correlator, extract_message_id)
from vumi.transports.parlayx.client import PolicyException, ServiceException
from vumi.transports.parlayx.server import DeliveryStatus
from vumi.transports.parlayx.soaputil import perform_soap_request
from vumi.transports.parlayx.tests.utils import (
    create_sms_reception_element, create_sms_delivery_receipt)
from vumi.transports.tests.utils import TransportTestCase


class MockParlayXClient(object):
    """
    A mock ``ParlayXClient`` that doesn't involve real HTTP requests but
    instead uses canned responses.
    """
    def __init__(self, start_sms_notification=None, stop_sms_notification=None,
                 send_sms=None):
        if start_sms_notification is None:
            start_sms_notification = partial(succeed, None)
        if stop_sms_notification is None:
            stop_sms_notification = partial(succeed, None)
        if send_sms is None:
            send_sms = partial(succeed, 'request_message_id')

        self.responses = {
            'start_sms_notification': start_sms_notification,
            'stop_sms_notification': stop_sms_notification,
            'send_sms': send_sms}
        self.calls = []

    def _invoke_response(self, name, args):
        """
        Invoke the canned response for the method name ``name`` and log the
        invocation.
        """
        self.calls.append((name, args))
        return self.responses[name]()

    def start_sms_notification(self):
        return self._invoke_response('start_sms_notification', [])

    def stop_sms_notification(self):
        return self._invoke_response('stop_sms_notification', [])

    def send_sms(self, to_addr, content, message_id):
        return self._invoke_response(
            'send_sms', [to_addr, content, message_id])


class ParlayXTransportTestCase(TransportTestCase):
    """
    Tests for `vumi.transports.parlayx.ParlayXTransport`.
    """
    transport_class = ParlayXTransport
    timeout = 1

    @inlineCallbacks
    def setUp(self):
        super(ParlayXTransportTestCase, self).setUp()
        self.port = 9999
        config = {
            'transport_name': self.transport_name,
            'web_notification_path': '/hello',
            'web_notification_port': self.port,
            'notification_endpoint_uri': 'endpoint_uri',
            'short_code': '54321',
            'remote_send_uri': 'send_uri',
            'remote_notification_uri': 'notification_uri'}
        self.uri = 'http://localhost:%s%s' % (
            self.port, config['web_notification_path'])

        def _create_client(transport, config):
            return MockParlayXClient()
        self.patch(self.transport_class, '_create_client', _create_client)
        self.transport = yield self.get_transport(config, start=False)

    @inlineCallbacks
    def test_ack(self):
        """
        Basic message delivery.
        """
        yield self.transport.startWorker()
        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        [event] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'ack')
        self.assertEqual(event['user_message_id'], msg['message_id'])

    @inlineCallbacks
    def test_nack(self):
        """
        Exceptions raised in an outbound message handler result in the message
        delivery failing, and a failure event being logged.
        """
        def _create_client(transport, config):
            return MockParlayXClient(
                send_sms=partial(fail, ValueError('failed')))
        self.patch(self.transport_class, '_create_client', _create_client)

        yield self.transport.startWorker()
        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        [event] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'failed')

        failures = self.flushLoggedErrors(ValueError)
        # Logged once by the transport and once by Twisted for being unhandled.
        self.assertEqual(2, len(failures))

    @inlineCallbacks
    def _test_nack_permanent(self, expected_exception):
        """
        The expected exception, when raised in an outbound message handler,
        results in a `PermanentFailure` and is logged along with the original
        exception.
        """
        def _create_client(transport, config):
            return MockParlayXClient(
                send_sms=partial(
                    fail, expected_exception('soapenv:Client', 'failed')))
        self.patch(self.transport_class, '_create_client', _create_client)

        yield self.transport.startWorker()
        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        [event] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(event['event_type'], 'nack')
        self.assertEqual(event['user_message_id'], msg['message_id'])
        self.assertEqual(event['nack_reason'], 'failed')

        failures = self.flushLoggedErrors(expected_exception, PermanentFailure)
        self.assertEqual(2, len(failures))

    def test_nack_service_exception(self):
        """
        When `ServiceException` is raised in an outbound message handler, it
        results in a `PermanentFailure` exception.
        """
        return self._test_nack_permanent(ServiceException)

    def test_nack_policy_exception(self):
        """
        When `PolicyException` is raised in an outbound message handler, it
        results in a `PermanentFailure` exception.
        """
        return self._test_nack_permanent(PolicyException)

    @inlineCallbacks
    def test_receive_sms(self):
        """
        When a text message is submitted to the Vumi ParlayX
        ``notifySmsReception`` SOAP endpoint, a message is
        published containing the message identifier, message content, from
        address and to address that accurately match what was submitted.
        """
        yield self.transport.startWorker()
        body = create_sms_reception_element(
            '1234', 'message', '+27117654321', '54321')
        yield perform_soap_request(self.uri, '', body)
        [msg] = self.get_dispatched_messages()
        self.assertEqual(
            ('1234', 'message', '+27117654321', '54321'),
            (msg['message_id'], msg['content'], msg['from_addr'],
             msg['to_addr']))

    @inlineCallbacks
    def test_delivery_receipt(self):
        """
        When a delivery receipt is submitted to the Vumi ParlayX
        ``notifySmsDeliveryReceiptResponse`` SOAP endpoint, an event is
        published containing the message identifier and the delivery status
        that accurately match what was submitted.
        """
        yield self.transport.startWorker()
        body = create_sms_delivery_receipt(
            '1234', '+27117654321', DeliveryStatus.DeliveredToNetwork)
        yield perform_soap_request(self.uri, '', body)
        [event] = self.get_dispatched_events()
        self.assertEqual(
            ('1234', 'delivered'),
            (event['user_message_id'], event['delivery_status']))


class TransportUtilsTests(TestCase):
    """
    Tests for miscellaneous functions in `vumi.transports.parlayx`.
    """
    def test_unique_correlator(self):
        """
        `unique_correlator` combines a Vumi transport message identifier and
        a UUID.
        """
        self.assertEqual(
            'arst:12341234', unique_correlator('arst', '12341234'))

    def test_extract_message_id(self):
        """
        `extract_message_id` splits a ParlayX correlator into a Vumi transport
        message identifier and a UUID.
        """
        self.assertEqual(
            ['arst', '12341234'], extract_message_id('arst:12341234'))
