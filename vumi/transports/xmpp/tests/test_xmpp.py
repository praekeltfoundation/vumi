from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.words.xish import domish

from vumi.transports.tests.test_base import TransportTestCase
from vumi.message import TransportUserMessage, from_json
from vumi.transports.xmpp.xmpp import XMPPTransport
from vumi.transports.xmpp.tests import test_xmpp_stubs


class XMPPTransportTestCase(TransportTestCase):

    transport_name = 'test_xmpp'

    @inlineCallbacks
    def mk_transport(self):
        transport = yield self.get_transport({
            'username': 'user@xmpp.domain.com',
            'password': 'testing password',
            'status': 'XMPP status',
            'host': 'xmpp.domain.com',
            'port': 5222,
            'transport_name': 'test_xmpp',
            'transport_type': 'xmpp',
        }, XMPPTransport, start=False)

        transport._xmpp_protocol = test_xmpp_stubs.TestXMPPTransportProtocol
        transport._xmpp_client = test_xmpp_stubs.TestXMPPClient
        yield transport.startWorker()
        yield transport.xmpp_protocol.connectionMade()
        self.jid = transport.jid
        returnValue(transport)

    @inlineCallbacks
    def test_outbound_message(self):
        transport = yield self.mk_transport()
        yield self.dispatch(TransportUserMessage(
            to_addr='user@xmpp.domain.com', from_addr='test@case.com',
            content='hello world', transport_name='test_xmpp',
            transport_type='xmpp', transport_metadata={}),
            rkey='test_xmpp.outbound')

        xmlstream = transport.xmpp_protocol.xmlstream
        self.assertEqual(len(xmlstream.outbox), 1)
        message = xmlstream.outbox[0]
        self.assertEqual(message['to'], 'user@xmpp.domain.com')
        self.assertTrue(message['id'])
        self.assertEqual(str(message.children[0]), 'hello world')

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
        dispatched_messages = self._amqp.get_dispatched('vumi',
            'test_xmpp.inbound')
        self.assertEqual(1, len(dispatched_messages))
        msg = from_json(dispatched_messages[0].body)
        self.assertEqual(msg['to_addr'], self.jid.userhost())
        self.assertEqual(msg['from_addr'], 'test@case.com')
        self.assertEqual(msg['transport_name'], 'test_xmpp')
        self.assertEqual(msg['message_id'], message['id'])
        self.assertEqual(msg['content'], 'hello world')
