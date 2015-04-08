# -*- coding: utf-8 -*-

"""Test for vumi.transport.truteq.truteq."""

from twisted.internet.defer import inlineCallbacks, Deferred, returnValue
from twisted.internet import reactor
from twisted.internet.task import deferLater
from twisted.test.proto_helpers import StringTransportWithDisconnection

from txssmi import constants as c
from txssmi.builder import SSMIRequest
from txssmi.commands import (
    Ack, USSDMessage, ExtendedUSSDMessage, SendUSSDMessage, MoMessage,
    ServerLogout)

from vumi.message import TransportUserMessage
from vumi.reconnecting_client import ReconnectingClientService
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.truteq.truteq import TruteqTransport


# To reduce verbosity.
SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_RESUME = TransportUserMessage.SESSION_RESUME
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_NONE = TransportUserMessage.SESSION_NONE


class StringTransportEndpoint(object):
    def __init__(self, connect_callback):
        self.connect_callback = connect_callback

    def connect(self, protocolFactory):
        string_transport = StringTransportWithDisconnection()
        protocol = protocolFactory.buildProtocol(None)
        protocol.makeConnection(string_transport)
        string_transport.protocol = protocol
        self.connect_callback(protocol)
        d = Deferred()
        d.callback(protocol)
        return d


class TestTruteqTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(TransportHelper(TruteqTransport))
        self.config = {
            'username': 'username',
            'password': 'password',
        }
        self.string_transport = StringTransportWithDisconnection()

        # NOTE: pausing the transport before starting so we can
        #       start the SSMIProtocol, which expects the vumi transport
        #       as an argument.
        self.transport = yield self.tx_helper.get_transport(
            self.config, start=False)
        st_endpoint = StringTransportEndpoint(self._ste_connect_callback)

        def truteq_service_maker(endpoint, factory):
            return ReconnectingClientService(st_endpoint, factory)

        self.transport.service_class = truteq_service_maker
        yield self.transport.startWorker()
        yield self.process_login_commands('username', 'password')

    def _ste_connect_callback(self, protocol):
        self.protocol = protocol
        self.string_transport = protocol.transport

    @inlineCallbacks
    def process_login_commands(self, username, password):
        [cmd] = yield self.receive(1)
        self.assertEqual(cmd.command_name, 'LOGIN')
        self.assertEqual(cmd.username, username)
        self.assertEqual(cmd.password, password)
        self.send(Ack(ack_type='1'))
        [link_check] = yield self.receive(1)
        self.assertEqual(link_check.command_name, 'LINK_CHECK')
        returnValue(True)

    def send(self, command):
        return self.protocol.lineReceived(str(command))

    def receive(self, count, clear=True):
        d = Deferred()

        def check_for_input():
            if not self.string_transport.value():
                reactor.callLater(0, check_for_input)
                return

            lines = self.string_transport.value().split(
                self.protocol.delimiter)
            commands = map(SSMIRequest.parse, filter(None, lines))
            if len(commands) >= count:
                if clear:
                    self.string_transport.clear()
                    self.string_transport.write(
                        self.protocol.delimiter.join(
                            map(str, commands[count:])))
                d.callback(commands[:count])

        check_for_input()

        return d

    def incoming_ussd(self, msisdn="12345678", ussd_type=c.USSD_RESPONSE,
                      phase="ignored", message="Hello"):
        self.send(USSDMessage(
            msisdn=msisdn, type=ussd_type, phase=c.USSD_PHASE_UNKNOWN,
            message=message))

    @inlineCallbacks
    def start_ussd(self, message="*678#", **kw):
        kw.setdefault("msisdn", "12345678")
        kw.setdefault("phase", c.USSD_PHASE_UNKNOWN)
        yield self.transport.handle_raw_inbound_message(
            USSDMessage(type=c.USSD_NEW, message=message, **kw))
        self.tx_helper.clear_dispatched_inbound()

    @inlineCallbacks
    def check_msg(self, from_addr="+12345678", to_addr="*678#", content=None,
                  session_event=None, helper_metadata=None):
        default_hmd = {'truteq': {'genfields': {}}}
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['transport_type'], 'ussd')
        self.assertEqual(msg['transport_metadata'], {})
        self.assertEqual(
            msg['helper_metadata'], helper_metadata or default_hmd)
        self.assertEqual(msg['from_addr'], from_addr)
        self.assertEqual(msg['to_addr'], to_addr)
        self.assertEqual(msg['content'], content)
        self.assertEqual(msg['session_event'], session_event)
        self.tx_helper.clear_dispatched_inbound()

    @inlineCallbacks
    def test_handle_inbound_ussd_new(self):
        yield self.send(USSDMessage(msisdn='27000000000', type=c.USSD_NEW,
                                    message='*678#', phase=c.USSD_PHASE_1))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['to_addr'], '*678#')
        self.assertEqual(msg['session_event'], SESSION_NEW)
        self.assertEqual(msg['transport_type'], 'ussd')

    @inlineCallbacks
    def test_handle_inbound_extended_ussd_new(self):
        yield self.send(ExtendedUSSDMessage(
            msisdn='27000000000', type=c.USSD_NEW, message='*678#',
            genfields='::3', phase=c.USSD_PHASE_1))
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['from_addr'], '+27000000000')
        self.assertEqual(msg['to_addr'], '*678#')
        self.assertEqual(msg['helper_metadata'], {
            'truteq': {
                'genfields': {
                    'IMSI': '',
                    'OperatorID': '3',
                    'SessionID': '',
                    'Subscriber Type': '',
                    'ValiPort': '',
                }
            }
        })

    @inlineCallbacks
    def test_handle_remote_logout(self):
        cmd = ServerLogout(ip='127.0.0.1')
        with LogCatcher() as logger:
            yield self.send(cmd)
            [warning] = logger.messages()
            self.assertEqual(
                warning,
                "Received remote logout command: %r" % (cmd,))

    @inlineCallbacks
    def test_handle_inbound_ussd_resume(self):
        yield self.start_ussd()
        self.incoming_ussd(ussd_type=c.USSD_RESPONSE, message="Hello")
        yield self.check_msg(content="Hello", session_event=SESSION_RESUME)

    @inlineCallbacks
    def test_handle_inbound_ussd_close(self):
        yield self.start_ussd()
        self.incoming_ussd(ussd_type=c.USSD_END, message="Done")
        yield self.check_msg(content="Done", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_inbound_ussd_timeout(self):
        yield self.start_ussd()
        self.incoming_ussd(ussd_type=c.USSD_TIMEOUT, message="Timeout")
        yield self.check_msg(content="Timeout", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_inbound_ussd_non_ascii(self):
        yield self.start_ussd()
        self.incoming_ussd(
            ussd_type=c.USSD_TIMEOUT, message=u"föóbær".encode("iso-8859-1"))
        yield self.check_msg(content=u"föóbær", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def test_handle_inbound_ussd_with_comma_in_content(self):
        yield self.start_ussd()
        self.incoming_ussd(ussd_type=c.USSD_TIMEOUT, message=u"foo, bar")
        yield self.check_msg(content=u"foo, bar", session_event=SESSION_CLOSE)

    @inlineCallbacks
    def _test_outbound_ussd(self, vumi_session_type, ssmi_session_type,
                            content="Test", encoding="utf-8"):
        yield self.tx_helper.make_dispatch_outbound(
            content, to_addr=u"+1234", session_event=vumi_session_type)
        [ussd_call] = yield self.receive(1)
        data = content.encode(encoding) if content else ""
        self.assertEqual(ussd_call.message, data)
        self.assertTrue(isinstance(ussd_call.message, str))
        self.assertEqual(ussd_call.msisdn, '1234')
        self.assertEqual(ussd_call.type, ssmi_session_type)

    def test_handle_outbound_ussd_no_session(self):
        return self._test_outbound_ussd(SESSION_NONE, c.USSD_RESPONSE)

    def test_handle_outbound_ussd_null_content(self):
        return self._test_outbound_ussd(SESSION_NONE, c.USSD_RESPONSE,
                                        content=None)

    def test_handle_outbound_ussd_resume(self):
        return self._test_outbound_ussd(SESSION_RESUME, c.USSD_RESPONSE)

    def test_handle_outbound_ussd_close(self):
        return self._test_outbound_ussd(SESSION_CLOSE, c.USSD_END)

    def test_handle_outbound_ussd_non_ascii(self):
        return self._test_outbound_ussd(
            SESSION_NONE, c.USSD_RESPONSE, content=u"föóbær",
            encoding='iso-8859-1')

    @inlineCallbacks
    def _test_content_wrangling(self, submitted, expected):
        yield self.tx_helper.make_dispatch_outbound(
            submitted, to_addr=u"+1234", session_event=SESSION_NONE)
        # Grab what was sent to Truteq
        [ussd_call] = yield self.receive(1)
        expected_msg = SendUSSDMessage(msisdn='1234', message=expected,
                                       type=c.USSD_RESPONSE)

        self.assertEqual(ussd_call, expected_msg)

    def test_handle_outbound_ussd_with_comma_in_content(self):
        return self._test_content_wrangling(
            'hello world, universe', 'hello world, universe')

    def test_handle_outbound_ussd_with_crln_in_content(self):
        return self._test_content_wrangling(
            'hello\r\nwindows\r\nworld', 'hello\nwindows\nworld')

    def test_handle_outbound_ussd_with_cr_in_content(self):
        return self._test_content_wrangling(
            'hello\rold mac os\rworld', 'hello\nold mac os\nworld')

    @inlineCallbacks
    def test_ussd_addr_retains_asterisks_and_hashes(self):
        self.incoming_ussd(ussd_type=c.USSD_NEW, message="*6*7*8#")
        yield self.check_msg(to_addr="*6*7*8#", session_event=SESSION_NEW)

    @inlineCallbacks
    def test_ussd_addr_appends_hashes_if_missing(self):
        self.incoming_ussd(ussd_type=c.USSD_NEW, message="*6*7*8")
        yield self.check_msg(to_addr="*6*7*8#", session_event=SESSION_NEW)

    @inlineCallbacks
    def test_handle_inbound_sms(self):
        cmd = MoMessage(msisdn='foo', message='bar', sequence='1')
        with LogCatcher() as logger:
            yield self.send(cmd)
            [warning] = logger.messages()
            self.assertEqual(
                warning[:59],
                "Received unsupported message, dropping: <MO command_id=103 ")
        self.flushLoggedErrors()

    @inlineCallbacks
    def test_reconnect(self):
        """
        When disconnected, the transport should attempt to reconnect.

        We test this by stashing the current protocol instance, disconnecting
        it, and asserting that we get a new protocol instance with the usual
        login commands after reconnection.
        """
        self.transport.client_service.delay = 0
        old_protocol = self.protocol
        yield self.protocol.transport.loseConnection()
        yield deferLater(reactor, 0, lambda: None)  # Let the reactor run.
        self.assertNotEqual(old_protocol, self.protocol)
        yield self.process_login_commands('username', 'password')
