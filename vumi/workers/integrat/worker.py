from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.message import Message
from vumi.service import Worker
from vumi.utils import safe_routing_key
from vumi.errors import VumiError

class IntegratWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        log.msg('starting worker with ', self.config)
        self.publisher = yield self.publish_to('ussd.outbound.%(transport_name)s' % self.config)
        self.consumer = yield self.consume('ussd.inbound.%s.%s' % (
            self.config['transport_name'],
            safe_routing_key(self.config['ussd_code'])
        ), self.consume_message)
        
    
    def consume_message(self, message):
        data = message.payload
        handler = getattr(self, '%(transport_message_type)s' % data,
                            self.noop)
        handler(data)
    
    def noop(self, data):
        log.msg('Got', data, 'but not doing anything with it')
    
    def end(self, session_id, closing_text):
        return self.reply(session_id, closing_text, 1)
    
    def reply(self, session_id, reply_text, flag=0):
        self.publisher.publish_message(Message(**{
            'transport_session_id': session_id,
            'message': reply_text,
            'close': str(flag)
        }))
    
    def new_session(self, data):
        session_id = data['transport_session_id']
        msisdn = data['sender']
        self.reply(session_id, 'Hi %s, this is an echo service. ' % msisdn +
                                'Enter 0 to stop.')
    
    def resume_session(self, data):
        session_id = data['transport_session_id']
        message = data['message'].strip()
        if message == '0':
            self.end(session_id, 'Thanks!')
        else:
            self.reply(session_id, 'You said: %s' % message)
    
    def open_session(self, data):
        pass
    
    def close_session(self, data):
        pass