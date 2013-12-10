from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock
from twisted.words.xish import domish

from vumi.tests.helpers import VumiTestCase
from vumi.transports.xmpp.xmpp import XMPPTransport
from vumi.transports.xmpp.tests import test_xmpp_stubs
from vumi.transports.tests.helpers import TransportHelper


class TestXMPPTransport(VumiTestCase):

    @inlineCallbacks
    def mk_transport(self):
        self.tx_helper = self.add_helper(TransportHelper(XMPPTransport))
        transport = yield self.tx_helper.get_transport({
            'username': 'user@xmpp.domain.com',
            'password': 'testing password',
            'status': 'chat',
            'status_message': 'XMPP Transport',
            'host': 'xmpp.domain.com',
            'port': 5222,
            'transport_type': 'xmpp',
        }, start=False)

        transport._xmpp_protocol = test_xmpp_stubs.TestXMPPTransportProtocol
        transport._xmpp_client = test_xmpp_stubs.TestXMPPClient
        transport.ping_call.clock = Clock()
        transport.presence_call.clock = Clock()
        yield transport.startWorker()
        yield transport.xmpp_protocol.connectionMade()
        self.jid = transport.jid
        returnValue(transport)

    def assert_ack(self, ack, reply):
        self.assertEqual(ack.payload['event_type'], 'ack')
        self.assertEqual(ack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(ack.payload['sent_message_id'], reply['message_id'])

    @inlineCallbacks
    def test_outbound_message(self):
        transport = yield self.mk_transport()
        msg = yield self.tx_helper.make_dispatch_outbound(
            "hi", to_addr='user@xmpp.domain.com', from_addr='test@case.com')

        xmlstream = transport.xmpp_protocol.xmlstream
        self.assertEqual(len(xmlstream.outbox), 1)
        message = xmlstream.outbox[0]
        self.assertEqual(message['to'], 'user@xmpp.domain.com')
        self.assertTrue(message['id'])
        self.assertEqual(str(message.children[0]), 'hi')

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assert_ack(ack, msg)

    @inlineCallbacks
    def test_inbound_message(self):
        transport = yield self.mk_transport()

        message = domish.Element((None, "message"))
        message['to'] = self.jid.userhost()
        message['from'] = 'test@case.com'
        message.addUniqueId()
        message.addElement((None, 'body'), content='hello world')
        protocol = transport.xmpp_protocol
        protocol.onMessage(message)
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['to_addr'], self.jid.userhost())
        self.assertEqual(msg['from_addr'], 'test@case.com')
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertNotEqual(msg['message_id'], message['id'])
        self.assertEqual(msg['transport_metadata']['xmpp_id'], message['id'])
        self.assertEqual(msg['content'], 'hello world')

    @inlineCallbacks
    def test_message_without_id(self):
        transport = yield self.mk_transport()

        message = domish.Element((None, "message"))
        message['to'] = self.jid.userhost()
        message['from'] = 'test@case.com'
        message.addElement((None, 'body'), content='hello world')
        self.assertFalse(message.hasAttribute('id'))

        protocol = transport.xmpp_protocol
        protocol.onMessage(message)

        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertTrue(msg['message_id'])
        self.assertEqual(msg['transport_metadata']['xmpp_id'], None)

    @inlineCallbacks
    def test_pinger(self):
        """
        The transport's pinger should send a ping after the ping_interval.
        """
        transport = yield self.mk_transport()
        self.assertEqual(transport.ping_interval, 60)
        # The LoopingCall should be configured and started.
        self.assertEqual(transport.ping_call.f, transport.send_ping)
        self.assertEqual(transport.ping_call.a, ())
        self.assertEqual(transport.ping_call.kw, {})
        self.assertEqual(transport.ping_call.interval, 60)
        self.assertTrue(transport.ping_call.running)

        # Stub output stream
        xmlstream = test_xmpp_stubs.TestXMLStream()
        transport.xmpp_client.xmlstream = xmlstream
        transport.pinger.xmlstream = xmlstream

        # Ping
        transport.ping_call.clock.advance(59)
        self.assertEqual(xmlstream.outbox, [])
        transport.ping_call.clock.advance(2)
        self.assertEqual(len(xmlstream.outbox), 1, repr(xmlstream.outbox))

        [message] = xmlstream.outbox
        self.assertEqual(message['to'], u'user@xmpp.domain.com')
        self.assertEqual(message['type'], u'get')
        [child] = message.children
        self.assertEqual(child.toXml(), u"<ping xmlns='urn:xmpp:ping'/>")

    @inlineCallbacks
    def test_presence(self):
        """
        The transport's presence should be announced regularly.
        """
        transport = yield self.mk_transport()
        self.assertEqual(transport.presence_interval, 60)
        # The LoopingCall should be configured and started.
        self.assertFalse(transport.presence_call.running)

        # Stub output stream
        xmlstream = test_xmpp_stubs.TestXMLStream()
        transport.xmpp_client.xmlstream = xmlstream
        transport.xmpp_client._initialized = True
        transport.presence.xmlstream = xmlstream

        self.assertEqual(xmlstream.outbox, [])
        transport.presence.connectionInitialized()
        transport.presence_call.clock.advance(1)
        self.assertEqual(len(xmlstream.outbox), 1, repr(xmlstream.outbox))

        self.assertEqual(transport.presence_call.f, transport.send_presence)
        self.assertEqual(transport.presence_call.a, ())
        self.assertEqual(transport.presence_call.kw, {})
        self.assertEqual(transport.presence_call.interval, 60)
        self.assertTrue(transport.presence_call.running)

        [presence] = xmlstream.outbox
        self.assertEqual(presence.toXml(),
            u"<presence><status>chat</status></presence>")

    @inlineCallbacks
    def test_normalizing_from_addr(self):
        transport = yield self.mk_transport()

        message = domish.Element((None, "message"))
        message['to'] = self.jid.userhost()
        message['from'] = 'test@case.com/some_xmpp_id'
        message.addUniqueId()
        message.addElement((None, 'body'), content='hello world')
        protocol = transport.xmpp_protocol
        protocol.onMessage(message)
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['from_addr'], 'test@case.com')
        self.assertEqual(msg['transport_metadata']['xmpp_id'], message['id'])
