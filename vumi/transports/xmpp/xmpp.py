from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.words.protocols.jabber.jid import JID
from twisted.words.xish import domish
from twisted.words.xish.domish import Element as DomishElement
from twisted.application.service import MultiService

from wokkel import client
from wokkel.xmppim import (RosterClientProtocol, MessageProtocol,
                           PresenceClientProtocol)

from vumi.service import Worker
from vumi.transports.base import Transport
from vumi.message import Message


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


class XMPPTransportProtocol(MessageProtocol):
    def __init__(self, jid, callback):
        super(MessageProtocol, self).__init__()
        self.jid = jid
        self.callback = callback

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
        self.callback(to_addr=self.jid.userhost(), from_addr=message['from'],
            content=text, transport_type='xmpp', )


class XMPPTransport(Transport):
    """
    The XMPPTransport for Gtalk
    """

    def setup_transport(self):
        log.msg("Starting XMPPTransport: %s" % self.transport_name)
        username = self.config.pop('username')
        password = self.config.pop('password')
        status = {None: self.config.pop('status')}
        host = self.config.pop('host')
        port = self.config.pop('port')

        self.xmpp_service = MultiService()

        jid = JID(username)
        xmpp_client = client.XMPPClient(jid, password, host, port)
        xmpp_client.logTraffic = self.config.get('debug')
        xmpp_client.setServiceParent(self.xmpp_service)

        presence = TransportPresenceClientProtocol()
        presence.setHandlerParent(xmpp_client)
        presence.available(statuses=status)

        roster = TransportRosterClientProtocol()
        roster.setHandlerParent(xmpp_client)

        self.xmpp_protocol = XMPPTransportProtocol(jid, self.publish_message)
        self.xmpp_protocol.setHandlerParent(xmpp_client)

        self.xmpp_service.startService()

        log.msg("XMPPTransport %s started." % self.transport_name)
    
    def teardown_transport(self):
        self.xmpp_service.stopService()
    
    def handle_outbound_message(self, message):
        log.msg("Consumed Message %s" % message)
        recipient = message['to_addr']
        text = message['content']

        jid = JID(recipient).userhost()
        
        if not self.xmpp_protocol.xmlstream:
            log.err("Outbound undeliverable, XMPP not initialized yet.")
            return False
        else:
            self.xmpp_protocol.reply(jid, text)

