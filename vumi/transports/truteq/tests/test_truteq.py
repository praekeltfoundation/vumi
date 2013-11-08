# -*- coding: utf-8 -*-

"""Test for vumi.transport.truteq.truteq."""

from twisted.internet.defer import inlineCallbacks, DeferredQueue, Deferred
from twisted.internet import reactor
from twisted.protocols.basic import LineReceiver
from twisted.internet.protocol import ServerFactory

from ssmi import client

from vumi.message import TransportUserMessage
from vumi.transports.tests.utils import TransportTestCase
from vumi.transports.truteq.truteq import TruteqTransport
from vumi.tests.utils import LogCatcher
from vumi.transports.tests.helpers import TransportHelper


# To reduce verbosity.
SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_RESUME = TransportUserMessage.SESSION_RESUME
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_NONE = TransportUserMessage.SESSION_NONE

SSMI_EXISTING = client.SSMI_USSD_TYPE_EXISTING
SSMI_NEW = client.SSMI_USSD_TYPE_NEW
SSMI_END = client.SSMI_USSD_TYPE_END
SSMI_TIMEOUT = client.SSMI_USSD_TYPE_TIMEOUT


class FakeSSMI(LineReceiver):
    delimiter = '\r'

    def lineReceived(self, line):
        parts = line.split(',')
        assert parts.pop(0) == 'SSMI'
        self.handle_ssmi(*parts)

    def handle_ssmi(self, command, *params):
        if command == '1':
            # Login, nothing to do.
            pass
        elif command == '110':
            # USSD, stash it on the factory.
            msisdn, ussd_type = params[:2]
            content = ','.join(params[2:])
            self.factory.ussd_calls.put((msisdn, content, ussd_type))
        else:
            raise ValueError("Unexpected command: %s %r" % (command, params))

    def send_ssmi(self, *args):
        self.transport.write("SSMI,%s\r" % ','.join(args))


class FakeSSMIFactory(ServerFactory):
    protocol = FakeSSMI

    def __init__(self):
        self.ussd_calls = DeferredQueue()


class TestTruteqTransport(TransportTestCase):

    transport_name = 'test_truteq_transport'
    transport_class = TruteqTransport

    @inlineCallbacks
    def setUp(self):
        super(TestTruteqTransport, self).setUp()
        self.server_factory = FakeSSMIFactory()
        self.fake_server = reactor.listenTCP(
            0, self.server_factory, interface='localhost')
        addr = self.fake_server.getHost()
        self.config = {
            'username': 'vumitest',
            'password': 'notarealpassword',
            'host': addr.host,
            'port': addr.port,
            }
        self.tx_helper = TransportHelper(self)
        self.add_cleanup(self.tx_helper.cleanup)
        self.transport = yield self.tx_helper.get_transport(self.config)

    @inlineCallbacks
    def tearDown(self):
        yield self.fake_server.stopListening()
        yield super(TestTruteqTransport, self).tearDown()

    def _incoming_ussd(self, msisdn="+12345", ussd_type=SSMI_EXISTING,
                       phase="ignored", message="Hello"):
        return self.transport.ussd_callback(msisdn, ussd_type, phase, message)

    def _start_ussd(self, message="*678#", **kw):
        self._incoming_ussd(ussd_type=SSMI_NEW, message=message, **kw)
        return self._check_msg(session_event=SESSION_NEW)

    @inlineCallbacks
    def _check_msg(self, from_addr="+12345", to_addr="*678#", content=None,
                   session_event=None, helper_metadata=None):
        default_hmd = {'truteq': {'genfields': {}}}
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], 'ussd')
        self.assertEqual(msg['transport_metadata'], {})
        self.assertEqual(msg['helper_metadata'],
                         helper_metadata or default_hmd)
        self.assertEqual(msg['from_addr'], from_addr)
        self.assertEqual(msg['to_addr'], to_addr)
        self.assertEqual(msg['content'], content)
        self.assertEqual(msg['session_event'], session_event)
        self.tx_helper.clear_dispatched_inbound()

    @inlineCallbacks
    def test_handle_inbound_ussd_new(self):
        yield self._incoming_ussd(ussd_type=SSMI_NEW, message="*678#")
        yield self._check_msg(to_addr="*678#", session_event=SESSION_NEW)

    @inlineCallbacks
    def test_handle_inbound_extended_ussd_new(self):
        yield self.transport.ussd_callback(
            '+12345', SSMI_NEW, 'ignored', '*678#', {'OperatorID': '3'})
        yield self._check_msg(
            to_addr="*678#", session_event=SESSION_NEW,
            helper_metadata={
                'truteq': {
                    'genfields': {
                        'OperatorID': '3'
                    }
                }
            })

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
        yield self.tx_helper.make_dispatch_outbound(
            content, to_addr=u"+1234", session_event=vumi_session_type)
        ussd_call = yield self.server_factory.ussd_calls.get()
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

    # The SSMI client doesn't actually like this.
    # def test_handle_outbound_ussd_new(self):
    #     return self._test_outbound_ussd(SESSION_NEW, SSMI_NEW)

    def test_handle_outbound_ussd_close(self):
        return self._test_outbound_ussd(SESSION_CLOSE, SSMI_END)

    def test_handle_outbound_ussd_non_ascii(self):
        return self._test_outbound_ussd(
            SESSION_NONE, SSMI_EXISTING, content=u"föóbær")

    @inlineCallbacks
    def _test_content_wrangling(self, submitted, expected):
        yield self.tx_helper.make_dispatch_outbound(
            submitted, to_addr=u"+1234", session_event=SESSION_NONE)
        # Grab what was sent to Truteq
        ussd_call = yield self.server_factory.ussd_calls.get()
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
        yield self._incoming_ussd(ussd_type=SSMI_NEW, message="*6*7*8#")
        yield self._check_msg(to_addr="*6*7*8#", session_event=SESSION_NEW)

    @inlineCallbacks
    def test_ussd_addr_appends_hashes_if_missing(self):
        yield self._incoming_ussd(ussd_type=SSMI_NEW, message="*6*7*8")
        yield self._check_msg(to_addr="*6*7*8#", session_event=SESSION_NEW)

    def test_ssmi_reconnect(self):
        d_fired = Deferred()
        d_fired.callback(None)
        new_client = client.SSMIClient()
        self.transport._setup_ssmi_client(new_client, d_fired)
        self.assertEqual(self.transport.ssmi_client, new_client)
