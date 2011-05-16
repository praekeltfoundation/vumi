from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.words.protocols.jabber.jid import JID
from twisted.words.xish import domish
from twisted.words.xish.domish import Element as DomishElement
from twisted.application.service import MultiService
from getpass import getpass

from wokkel import client
from wokkel.xmppim import RosterClientProtocol, MessageProtocol, PresenceClientProtocol, PresenceProtocol

from datetime import datetime

from vumi.service import Worker
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
    def __init__(self, jid, publisher):
        super(MessageProtocol, self).__init__()
        self.publisher = publisher
    
    def reply(self, jid, content):
        message = domish.Element((None, "message"))
        # intentionally leaving from blank, leaving for XMPP server
        # to figure out
        message['to'] = jid
        message['type'] = 'chat'
        message.addUniqueId()
        message.addElement((None,'body'), content=content)
        log.msg('sending message "%s" to "%s"' % (content, jid.userhost()))
        self.xmlstream.send(message)

    def onMessage(self, message):
        """Messages sent to the bot will arrive here. Command handling routing
        is done in this function."""
        if not isinstance(message.body, DomishElement):
            return None

        sender = JID(message['from']).userhost()
        text = unicode(message.body).encode('utf-8').strip()
        self.publisher.publish_message(Message(sender=message['from'], message=text))


class XMPPTransport(Worker):
    """
    The XMPPTransport for Gtalk
    """
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPTransport for %s" % self.config['username'])
        
        username = self.config.pop('username')
        password = self.config.pop('password')
        status = {None: self.config.pop('status')}
        host = self.config.pop('host')
        port = self.config.pop('port')
        
        self.publisher = yield self.publish_to('xmpp.inbound.gtalk.%s' % username)
        
        s = MultiService()
        
        jid = JID(username)
        xmpp_client = client.XMPPClient(jid, password, host, port)
        xmpp_client.logTraffic = self.config.get('debug')
        xmpp_client.setServiceParent(s)
        
        presence = TransportPresenceClientProtocol()
        presence.setHandlerParent(xmpp_client)
        presence.available(statuses=status)
        
        roster = TransportRosterClientProtocol()
        roster.setHandlerParent(xmpp_client)
        
        self.xmpp_protocol = XMPPTransportProtocol(jid, self.publisher)
        self.xmpp_protocol.setHandlerParent(xmpp_client)
        
        self.consume("xmpp.outbound.gtalk.%s" % jid.userhost(), 
                                self.consume_message)
        
        s.startService()
        
        log.msg("XMPPTransport started.")
    
    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        dictionary = message.payload
        jid = JID(dictionary.get('recipient')).userhost()
        text = dictionary.get('message','')
        
        if not self.xmpp_protocol.xmlstream:
            log.err("Outbound undeliverable, XMPP not initialized yet.")
            return False
        else:
            self.xmpp_protocol.reply(jid, text)
    
    def stopWorker(self):
        log.msg("Stopping the XMPPTransport")
    


