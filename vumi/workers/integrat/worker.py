from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.message import Message
from vumi.service import Worker
from vumi.errors import VumiError
from vumi.workers.integrat.utils import HigateXMLParser

hxp = HigateXMLParser()

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
        hgmsg = hxp.parse(xml_message.get('content'))
        log.msg(hgmsg)
        
        event_name = hgmsg.get('EventType','').lower()
        handler = getattr(self, 'handle_%s' % event_name, self.noop)
        msg = Message(uuid=uuid,message=handler(hgmsg))
        self.publisher.publish_message(msg)
    
    def noop(self, hgmsg):
        return ''
    
    def end(self, session_id, closing_text):
        return self.reply(session_id, closing_text, 1)
    
    def reply(self, session_id, reply_text, flag=0):
        return hxp.build({
            'Flags': str(flag),
            'SessionID': session_id,
            'Type': 'USSReply',
            'USSText': reply_text,
            'Password': self.config.get('password'),
            'UserID': self.config.get('username')
        })
    
    def handle_request(self, hgmsg):
        session_id = hgmsg['SessionID']
        text = hgmsg['USSText'].strip()
        if text == 'REQ':
            return self.new_session(session_id, text)
        return self.resume_session(session_id, text)
    
    def handle_open(self, hgmsg):
        session_id = hgmsg['SessionID']
        self.session_started(session_id)
    
    def handle_close(self, hgmsg):
        session_id = hgmsg['SessionID']
        self.session_ended(session_id)
    
    def session_started(self, session_id):
        log.msg('session started for %s' % session_id)
    
    def session_ended(self, session_id):
        log.msg('session ended for %s' % session_id)
    
    def new_session(self, session_id, text):
        return self.reply(session_id, 'hi there new %s' % session_id)
    
    def resume_session(self, session_id, text):
        return self.end(session_id, 'you said %s. Bye!' % text)
    
