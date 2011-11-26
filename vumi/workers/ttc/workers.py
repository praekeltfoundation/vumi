# -*- test-case-name: vumi.workers.ttc.tests.test_ttc -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.application import ApplicationWorker
from vumi.message import Message

class TtcGenericWorker(ApplicationWorker):
    
    @inlineCallbacks
    def startWorker(self):
        super(TtcGenericWorker, self).startWorker()
        self.control_consumer = yield self.consume(
            '%(transport_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
        
    
    def consume_user_message(self, message):
        log.msg("User message: %s" % message['content'])
        
    def consume_control(self, message):
        log.msg("Control message!")
        #data = message.payload['data']
        program = message.get('program', {})
        name = program.get('name')
        group = program.get('group',{})
        number = group.get('number')
        log.msg("Control message %s to be add %s" % (number,name))
    
    def dispatch_event(self, message):
        log.msg("Event message!")
    