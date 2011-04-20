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

from vumi.service import Worker, Publisher
from vumi.message import Message

class TransportRosterClientProtocol(RosterClientProtocol):
    
    def connectionInitialized(self):
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
        self.jid = jid
        self.publisher = publisher
        self.routing_key = "%s.%s" % (self.publisher.routing_key, 
                                        self.jid.userhost())
    
    def reply(self, jid, content):
        message = domish.Element((None, "message"))
        # intentionally leaving from blank, leaving for XMPP server
        # to figure out
        message['to'] = jid
        message['type'] = 'chat'
        message.addUniqueId()
        message.addElement((None,'body'), content=content)
        self.xmlstream.send(message)

    def onMessage(self, message):
        """Messages sent to the bot will arrive here. Command handling routing
        is done in this function."""
        if not isinstance(message.body, DomishElement):
            return None

        sender = JID(message['from']).userhost()
        text = unicode(message.body).encode('utf-8').strip()
        self.publisher.publish_message(Message(sender=message['from'], 
            message=text), routing_key=self.routing_key)


class XMPPPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"             # -> route based on pattern matching
    routing_key = 'xmpp.inbound.gtalk'
    durable = True                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk
    

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
        
        self.publisher = yield self.start_publisher(XMPPPublisher)
        
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
            log.msg("Outbound undeliverable, XMPP not initialized yet.")
            raise Exception, 'Undeliverable, xmpp not ready. Sticking back on the queue.'
        else:
            self.xmpp_protocol.reply(jid, text)
    
    def stopWorker(self):
        log.msg("Stopping the XMPPTransport")
    


