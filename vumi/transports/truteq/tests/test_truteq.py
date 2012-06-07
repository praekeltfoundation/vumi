# -*- coding: utf-8 -*-

"""Test for vumi.transport.truteq.truteq."""

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.internet import reactor

from ssmi import client

from vumi.message import TransportUserMessage
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.truteq.truteq import TruteqTransport
from vumi.tests.utils import LogCatcher


# To reduce verbosity.
SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_RESUME = TransportUserMessage.SESSION_RESUME
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_NONE = TransportUserMessage.SESSION_NONE

SSMI_EXISTING = client.SSMI_USSD_TYPE_EXISTING
SSMI_NEW = client.SSMI_USSD_TYPE_NEW
SSMI_END = client.SSMI_USSD_TYPE_END
SSMI_TIMEOUT = client.SSMI_USSD_TYPE_TIMEOUT


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
                  ussd_type=SSMI_EXISTING):
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
            'redis': 'FAKE_REDIS',
            }
        self.transport = yield self.get_transport(self.config, start=True)

    def _incoming_ussd(self, msisdn="+12345", ussd_type=SSMI_EXISTING,
                       phase="ignored", message="Hello"):
        self.transport.ussd_callback(msisdn, ussd_type, phase, message)

    def _start_ussd(self, to_addr="+678", **kw):
        self._incoming_ussd(ussd_type=SSMI_NEW, message=to_addr, **kw)
        return self._check_msg(session_event=SESSION_NEW)

    @inlineCallbacks
    def _check_msg(self, from_addr="+12345", to_addr="+678", content=None,
                   session_event=None):
        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], 'ussd')
        self.assertEqual(msg['transport_metadata'], {})
        self.assertEqual(msg['from_addr'], from_addr)
        self.assertEqual(msg['to_addr'], to_addr)
        self.assertEqual(msg['content'], content)
        self.assertEqual(msg['session_event'], session_event)
        self.clear_dispatched_messages()

    @inlineCallbacks
    def test_handle_inbound_ussd_new(self):
        yield self._incoming_ussd(ussd_type=SSMI_NEW, message="+678")
        yield self._check_msg(to_addr="+678", session_event=SESSION_NEW)

    @inlineCallbacks
    def test_handle_inbound_ussd_resume(self):
        yield self._start_ussd()
        yield self._incoming_ussd(ussd_type=SSMI_EXISTING, message="Hello")
        yield self._check_msg(content="Hello", session_event=SESSION_RESUME)

    @inlineCallbacks
    def test_handle_inbound_ussd_close(self):
        yield self._start_ussd()
        yield self._incoming_ussd(ussd_type=SSMI_END, message="Done")
        yield self._check_msg(content="Done", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_inbound_ussd_timeout(self):
        yield self._start_ussd()
        yield self._incoming_ussd(ussd_type=SSMI_TIMEOUT, message="Timeout")
        yield self._check_msg(content="Timeout", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_inbound_ussd_non_ascii(self):
        yield self._start_ussd()
        yield self._incoming_ussd(
            ussd_type=SSMI_TIMEOUT, message=u"föóbær".encode("utf-8"))
        yield self._check_msg(content=u"föóbær", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def _test_outbound_ussd(self, vumi_session_type, ssmi_session_type,
                            content="Test", encoding="utf-8"):
        msg = self.mkmsg_out(content=content, to_addr=u"+1234",
                             session_event=vumi_session_type)
        yield self.dispatch(msg)
        ussd_call = yield self.dummy_connect.ussd_calls.get()
        data = content.encode(encoding) if content else ""
        self.assertFalse(
            # This stuff all needs to be bytestrings by here.
            any(isinstance(field, unicode) for field in ussd_call))
        self.assertEqual(ussd_call, ("1234", data, ssmi_session_type))

    def test_handle_outbound_ussd_no_session(self):
        return self._test_outbound_ussd(SESSION_NONE, SSMI_EXISTING)

    def test_handle_outbound_ussd_null_content(self):
        return self._test_outbound_ussd(SESSION_NONE, SSMI_EXISTING,
                                        content=None)

    def test_handle_outbound_ussd_resume(self):
        return self._test_outbound_ussd(SESSION_RESUME, SSMI_EXISTING)

    def test_handle_outbound_ussd_new(self):
        return self._test_outbound_ussd(SESSION_NEW, SSMI_NEW)

    def test_handle_outbound_ussd_close(self):
        return self._test_outbound_ussd(SESSION_CLOSE, SSMI_END)

    def test_handle_outbound_ussd_non_ascii(self):
        return self._test_outbound_ussd(
            SESSION_NONE, SSMI_EXISTING, content=u"föóbær")

    @inlineCallbacks
    def _test_content_wrangling(self, submitted, expected):
        msg = self.mkmsg_out(content=submitted,
            to_addr=u"+1234", session_event=SESSION_NONE)
        yield self.dispatch(msg)
        # Grab what was sent to Truteq
        ussd_call = yield self.dummy_connect.ussd_calls.get()
        data = expected.encode("utf-8")
        self.assertFalse(
            # This stuff all needs to be bytestrings by here.
            any(isinstance(field, unicode) for field in ussd_call))
        self.assertEqual(ussd_call, ("1234", data,
                                        SSMI_EXISTING))

    def test_handle_outbound_ussd_with_crln_in_content(self):
        return self._test_content_wrangling(
            'hello\r\nwindows\r\nworld', 'hello\nwindows\nworld')

    def test_handle_outbound_ussd_with_cr_in_content(self):
        return self._test_content_wrangling(
            'hello\rold mac os\rworld', 'hello\nold mac os\nworld')

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

    @inlineCallbacks
    def test_ussd_addr_retains_asterisks_and_hashes(self):
        yield self._incoming_ussd(ussd_type=SSMI_NEW, message="+6*7*8#")
        yield self._check_msg(to_addr="+6*7*8#", session_event=SESSION_NEW)
