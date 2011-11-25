# -*- test-case-name: vumi.workers.ttc.tests.test_ttc -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.application import ApplicationWorker

class TtcGenericWorker(ApplicationWorker):
    
    @inlineCallbacks
    def start_worker(self):
        super(TtcGenericWorker, self).start_worker();
        self.control_consumer = yield self.consume(
            '%(transport_name)s.control' % self.config,
            self.control,
            message_class=Message)
        
    
    def consume_user_message(self, message):
        log.msg("User message: %s" % message['content'])
        
    def control(self,message):
        log.msg("Control message!")
    
    def dispatch_event(self, message):
        log.msg("Event message!")
    