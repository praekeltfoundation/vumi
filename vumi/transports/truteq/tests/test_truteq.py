"""Test for vumi.transport.truteq.truteq."""

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.internet import reactor

from ssmi import client

from vumi.message import TransportUserMessage
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.truteq.truteq import TruteqTransport
from vumi.tests.utils import LogCatcher, FakeRedis


class MockConnectTCPForSSMI(object):
    def __init__(self):
        self.ussd_calls = DeferredQueue()
        self.clear()

    def __call__(self, host, port, factory):
        self.host, self.port, self.factory = host, port, factory
        ssmi_client = factory.buildProtocol("dummy_address")
        ssmi_client.send_ussd = self.send_ussd
        return self

    def clear(self):
        self.host, self.port, self.factory = None, None, None

    def send_ussd(self, msisdn, message,
                  ussd_type=client.SSMI_USSD_TYPE_EXISTING):
        self.ussd_calls.put((msisdn, message, ussd_type))

    def disconnect(self):
        self.clear()


class TestTruteqTransport(TransportTestCase):

    transport_name = 'test_truteq_transport'
    transport_class = TruteqTransport

    @inlineCallbacks
    def setUp(self):
        super(TestTruteqTransport, self).setUp()
        self.dummy_connect = MockConnectTCPForSSMI()
        self.patch(reactor, 'connectTCP', self.dummy_connect)
        self.config = {
            'username': 'vumitest',
            'password': 'notarealpassword',
            'host': 'localhost',
            'port': 1234,
            }
        self.transport = yield self.get_transport(self.config, start=True)
        self.transport.r_server = FakeRedis()

    def tearDown(self):
        super(TestTruteqTransport, self).tearDown()
        self.transport.r_server.teardown()

    def _incoming_ussd(self, msisdn="+12345",
                       ussd_type=client.SSMI_USSD_TYPE_EXISTING,
                       phase="ignored", message="Hello"):
        self.transport.ussd_callback(msisdn, ussd_type, phase, message)

    def _start_ussd(self, to_addr="+678", **kw):
        self._incoming_ussd(ussd_type=client.SSMI_USSD_TYPE_NEW,
                            message=to_addr, **kw)
        self._check_msg(session_event=TransportUserMessage.SESSION_NEW)

    def _check_msg(self, from_addr="+12345", to_addr="+678", content=None,
                    session_event=None):
        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], 'ussd')
        self.assertEqual(msg['transport_metadata'], {})
        self.assertEqual(msg['from_addr'], from_addr)
        self.assertEqual(msg['to_addr'], to_addr)
        self.assertEqual(msg['content'], content)
        self.assertEqual(msg['session_event'], session_event)
        self.clear_dispatched_messages()

    def test_handle_inbound_ussd_new(self):
        self._incoming_ussd(ussd_type=client.SSMI_USSD_TYPE_NEW,
                            message="+678")
        self._check_msg(to_addr="+678",
                        session_event=TransportUserMessage.SESSION_NEW)

    def test_handle_inbound_ussd_resume(self):
        self._start_ussd()
        self._incoming_ussd(ussd_type=client.SSMI_USSD_TYPE_EXISTING,
                        message="Hello")
        self._check_msg(content="Hello",
                        session_event=TransportUserMessage.SESSION_RESUME)

    def test_handle_inbound_ussd_close(self):
        self._start_ussd()
        self._incoming_ussd(ussd_type=client.SSMI_USSD_TYPE_END,
                            message="Done")
        self._check_msg(content="Done",
                        session_event=TransportUserMessage.SESSION_CLOSE)

    def test_handle_inbound_ussd_timeout(self):
        self._start_ussd()
        self._incoming_ussd(ussd_type=client.SSMI_USSD_TYPE_TIMEOUT,
                        message="Timeout")
        self._check_msg(content="Timeout",
                        session_event=TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def _test_outbound_ussd(self, vumi_session_type, ssmi_session_type,
                            content="Test"):
        msg = self.mkmsg_out(content=content, to_addr="+1234",
                             session_event=vumi_session_type)
        yield self.dispatch(msg)
        ussd_call = yield self.dummy_connect.ussd_calls.get()
        self.assertEqual(ussd_call, ("+1234", content or "",
                                     ssmi_session_type))

    def test_handle_outbound_ussd_no_session(self):
        return self._test_outbound_ussd(TransportUserMessage.SESSION_NONE,
                                        client.SSMI_USSD_TYPE_EXISTING)

    def test_handle_outbound_ussd_null_content(self):
        return self._test_outbound_ussd(TransportUserMessage.SESSION_NONE,
                                        client.SSMI_USSD_TYPE_EXISTING,
                                        content=None)

    def test_handle_outbound_ussd_resume(self):
        return self._test_outbound_ussd(TransportUserMessage.SESSION_RESUME,
                                        client.SSMI_USSD_TYPE_EXISTING)

    def test_handle_outbound_ussd_new(self):
        return self._test_outbound_ussd(TransportUserMessage.SESSION_NEW,
                                        client.SSMI_USSD_TYPE_NEW)

    def test_handle_outbound_ussd_close(self):
        return self._test_outbound_ussd(TransportUserMessage.SESSION_CLOSE,
                                        client.SSMI_USSD_TYPE_END)

    def test_handle_inbound_sms(self):
        with LogCatcher() as logger:
            self.transport.sms_callback("foo", baz="bar")
            [error] = logger.errors
            self.assertEqual(error["message"][0],
                             repr("Got SMS from SSMI but SMSes not supported:"
                                  " ('foo',), {'baz': 'bar'}"))

    def test_handle_ssmi_error(self):
        with LogCatcher() as logger:
            self.transport.ssmi_errback("foo", baz="bar")
            [error] = logger.errors
            self.assertEqual(error["message"][0],
                             repr("Got error from SSMI:"
                                  " ('foo',), {'baz': 'bar'}"))
