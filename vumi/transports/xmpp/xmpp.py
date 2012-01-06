# -*- test-case-name: vumi.transports.xmpp.tests.test_xmpp -*-
# -*- encoding: utf-8 -*-

from twisted.python import log
from twisted.words.protocols.jabber.jid import JID
from twisted.words.xish import domish
from twisted.words.xish.domish import Element as DomishElement

from wokkel.client import XMPPClient
from wokkel.xmppim import (RosterClientProtocol, MessageProtocol,
                           PresenceClientProtocol)

from vumi.transports.base import Transport


class TransportRosterClientProtocol(RosterClientProtocol):

    def connectionInitialized(self):
        # get the roster as soon as the connection's been initialized, this
        # allows us to see who's online but more importantly, allows us to see
        # who's added us to their roster. This allows us to auto subscribe to
        # anyone, automatically adding them to our roster, skips the "user ...
        # wants to add you to their roster, allow? yes/no" hoopla.
        self.getRoster()


class TransportPresenceClientProtocol(PresenceClientProtocol):
    """
    A custom presence protocol to automatically accept any subscription
    attempt.
    """

    def subscribeReceived(self, entity):
        self.subscribe(entity)
        self.subscribed(entity)

    def unsubscribeReceived(self, entity):
        self.unsubscribe(entity)
        self.unsubscribed(entity)


class XMPPTransportProtocol(MessageProtocol, object):
    def __init__(self, jid, message_callback, connection_callback,
                 connection_lost_callback=None,):
        super(MessageProtocol, self).__init__()
        self.jid = jid
        self.message_callback = message_callback
        self.connection_callback = connection_callback
        self.connection_lost_callback = connection_lost_callback

    def reply(self, jid, content):
        message = domish.Element((None, "message"))
        # intentionally leaving from blank, leaving for XMPP server
        # to figure out
        message['to'] = jid
        message['type'] = 'chat'
        message.addUniqueId()
        message.addElement((None, 'body'), content=content)
        self.xmlstream.send(message)

    def onMessage(self, message):
        """Messages sent to the bot will arrive here. Command handling routing
        is done in this function."""
        if not isinstance(message.body, DomishElement):
            return None
        text = unicode(message.body).encode('utf-8').strip()
        self.message_callback(
            to_addr=self.jid.userhost(),
            from_addr=message['from'],
            content=text,
            transport_type='xmpp',
            message_id=message['id'])

    def connectionMade(self):
        self.connection_callback()
        return super(XMPPTransportProtocol, self).connectionMade()

    def connectionLost(self, reason):
        if self.connection_lost_callback is not None:
            self.connection_lost_callback(reason)
        log.msg("XMPP Connection lost.")
        super(XMPPTransportProtocol, self).connectionLost(reason)


class XMPPTransport(Transport):
    """
    The XMPPTransport for Gtalk
    """

    start_message_consumer = False
    _xmpp_protocol = XMPPTransportProtocol
    _xmpp_client = XMPPClient

    def setup_transport(self):
        log.msg("Starting XMPPTransport: %s" % self.transport_name)

        username = self.config.pop('username')
        password = self.config.pop('password')
        statuses = {None: self.config.pop('status', '')}
        host = self.config.pop('host')
        port = int(self.config.pop('port'))

        self.jid = JID(username)
        self.xmpp_client = self._xmpp_client(self.jid, password, host, port)
        self.xmpp_client.logTraffic = self.config.get('debug')
        self.xmpp_client.setServiceParent(self)

        presence = TransportPresenceClientProtocol()
        presence.setHandlerParent(self.xmpp_client)
        presence.available(statuses=statuses)

        roster = TransportRosterClientProtocol()
        roster.setHandlerParent(self.xmpp_client)

        self.xmpp_protocol = self._xmpp_protocol(
            self.jid, self.publish_message, self._setup_message_consumer)
        self.xmpp_protocol.setHandlerParent(self.xmpp_client)

        log.msg("XMPPTransport %s started." % self.transport_name)

    def teardown_transport(self):
        log.msg("XMPPTransport %s stopped." % self.transport_name)

    def handle_outbound_message(self, message):
        recipient = message['to_addr']
        text = message['content']

        jid = JID(recipient).userhost()

        if not self.xmpp_protocol.xmlstream:
            log.err("Outbound undeliverable, XMPP not initialized yet.")
            return False
        else:
            self.xmpp_protocol.reply(jid, text)
