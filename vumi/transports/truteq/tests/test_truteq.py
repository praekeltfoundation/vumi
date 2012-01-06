"""Test for vumi.transport.truteq.truteq."""

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from ssmi import client

from vumi.message import TransportUserMessage
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.truteq.truteq import TruteqTransport
from vumi.tests.utils import LogCatcher


class MockConnectTCP(object):
    def __init__(self):
        self.clear()

    def __call__(self, host, port, factory):
        self.host, self.port, self.factory = host, port, factory
        return self

    def clear(self):
        self.host, self.port, self.factory = None, None, None

    def disconnect(self):
        self.clear()


class TestTruteqTransport(TransportTestCase):

    transport_name = 'test_truteq_transport'
    transport_class = TruteqTransport

    @inlineCallbacks
    def setUp(self):
        super(TestTruteqTransport, self).setUp()
        self.dummy_connect = MockConnectTCP()
        self.patch(reactor, 'connectTCP', self.dummy_connect)
        self.config = {
            'username': 'vumitest',
            'password': 'notarealpassword',
            'host': 'localhost',
            'port': 1234,
            }
        self.transport = yield self.get_transport(self.config, start=True)

    def tearDown(self):
        super(TestTruteqTransport, self).tearDown()

    def _send_ussd(self, msisdn="+12345",
                   ussd_type=client.SSMI_USSD_TYPE_EXISTING,
                   phase="ignored", message="Hello"):
        self.transport.ussd_callback(msisdn, ussd_type, phase, message)

    def _start_ussd(self, to_addr="+678", **kw):
        self._send_ussd(ussd_type=client.SSMI_USSD_TYPE_NEW, message=to_addr,
                        **kw)
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
        self._send_ussd(ussd_type=client.SSMI_USSD_TYPE_NEW, message="+678")
        self._check_msg(to_addr="+678",
                        session_event=TransportUserMessage.SESSION_NEW)

    def test_handle_inbound_ussd_resume(self):
        self._start_ussd()
        self._send_ussd(ussd_type=client.SSMI_USSD_TYPE_EXISTING,
                        message="Hello")
        self._check_msg(content="Hello",
                        session_event=TransportUserMessage.SESSION_RESUME)

    def test_handle_inbound_ussd_close(self):
        self._start_ussd()
        self._send_ussd(ussd_type=client.SSMI_USSD_TYPE_END, message="Done")
        self._check_msg(content="Done",
                        session_event=TransportUserMessage.SESSION_CLOSE)

    def test_handle_inbound_ussd_timeout(self):
        self._start_ussd()
        self._send_ussd(ussd_type=client.SSMI_USSD_TYPE_TIMEOUT,
                        message="Timeout")
        self._check_msg(content="Timeout",
                        session_event=TransportUserMessage.SESSION_CLOSE)

    def test_handle_outbout_message(self):
        self.fail("Unimplemented test.")
