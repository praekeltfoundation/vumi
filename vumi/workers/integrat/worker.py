from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.message import Message
from vumi.service import Worker
from vumi.workers.integrat.utils import HigateXMLParser

class IntegratWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        log.msg('starting worker with ', self.config)
        self.publisher = yield self.publish_to(self.config['publish_key'])
        self.consumer = yield self.consume(self.config['consume_key'], 
                                            self.consume_message)
        
    
    def consume_message(self, message):
        uuid = message.payload.get('uuid')
        xml_message = message.payload.get('message')
        hgmsg = HigateXMLParser().parse(xml_message.get('content'))
        log.msg(hgmsg)
        handler = getattr(self, 'handle_%s' % hgmsg.get('USSText'), self.all_ok)
        log.msg('handler', handler)
        msg = Message(uuid=uuid,message=handler(hgmsg))
        log.msg('Publishing', msg)
        self.publisher.publish_message(msg)
    
    def all_ok(self, hgmsg):
        return u'<?xml version="1.0" encoding="utf-8"?>'
    
    def handle_REQ(self, hgmsg):
        return 'this is a REQ'