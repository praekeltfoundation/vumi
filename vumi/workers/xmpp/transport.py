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

from vumi.service import Worker, Consumer, Publisher
from vumi.message import Message

class TransportRosterClientProtocol(RosterClientProtocol):
    
    def connectionMade(self):
        reactor.callLater(2, self.getRoster)
    
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
        self.publisher.publish_message(Message(sender=message['from'], message=text))


class XMPPConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    queue_name = "xmpp.gtalk.outbound"
    routing_key = "xmpp.gtalk.outbound"
    
    def __init__(self, transport):
        self.transport = transport
    
    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        dictionary = message.payload
        jid = JID(dictionary.get('recipient')).userhost()
        text = unicode(dictionary.get('message','')).encode('utf-8').strip()
        self.transport.reply(jid, text)
    

class XMPPPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"             # -> route based on pattern matching
    routing_key = 'xmpp.gtalk.inbound'
    durable = True                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk
    
    def publish_json(self, dictionary, **kwargs):
        log.msg("Publishing JSON %s with extra args: %s" % (dictionary, kwargs))
        super(XMPPPublisher, self).publish_json(dictionary, **kwargs)
    

class XMPPTransport(Worker):
    """
    The XMPPTransport for Gtalk
    """
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the XMPPTransport for %s" % self.config['username'])
        
        username = self.config.pop('username')
        password = self.config.pop('password') or getpass('Password:')
        status = {None: self.config.pop('status')}
        host = self.config.pop('host')
        port = self.config.pop('port')
        
        self.publisher = yield self.start_publisher(XMPPPublisher)
        
        s = MultiService()
        
        jid = JID(username)
        xmpp_client = client.XMPPClient(jid, password, host, port)
        xmpp_client.logTraffic = False
        xmpp_client.setServiceParent(s)
        
        presence = TransportPresenceClientProtocol()
        presence.setHandlerParent(xmpp_client)
        presence.available(statuses=status)
        
        roster = TransportRosterClientProtocol()
        roster.setHandlerParent(xmpp_client)
        
        transport = XMPPTransportProtocol(jid, self.publisher)
        transport.setHandlerParent(xmpp_client)
        
        self.consumer = yield self.start_consumer(XMPPConsumer, transport)
        
        s.startService()
        
        log.msg("XMPPTransport started.")
    
    def stopWorker(self):
        log.msg("Stopping the XMPPTransport")
    


