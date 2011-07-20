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
        
        xml_response = self.handle_text(hgmsg):
        msg = Message(uuid=uuid,message=xml_response)
        self.publisher.publish_message(msg)
    
    def handle_ussd_event(self, hgmsg):
        if 'USSText' in hgmsg:
            return self.handle_text(hgmsg)
        else:
            return self.blank(hgmsg)
    
    def blank(self, hgmsg):
        log.msg('returning blank!')
        return u''
    
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
    
    def call(self, fn_name, *args):
        handler = getattr(self, fn_name)
        if not handler:
            raise VumiError, 'Please override %s(session_id, msg)' % fn_name
        return handler(*args) 
    
    def handle_text(self, hgmsg):
        if hgmsg['USSText'] == 'REQ':
            return self.call('new_session', hgmsg['SessionID'])
    
    def new_session(self, session_id):
        return self.reply(session_id, 'hi there new %s' % session_id)