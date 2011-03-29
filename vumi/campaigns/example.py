from vumi.services.truteq.base import Publisher, Consumer, SessionType
from vumi.services.worker import PubSubWorker
from twisted.python import log

class EchoConsumer(Consumer):
    
    def new_ussd_session(self, msisdn, message):
        self.reply(msisdn, "Hello, this is an echo service for testing. "
                            "Reply with whatever. Reply '0' to end session.", 
                            SessionType.existing)
    
    def existing_ussd_session(self, msisdn, message):
        if message == "0":
            self.reply(msisdn, "quitting, goodbye!", SessionType.end)
        else:
            self.reply(msisdn, message, SessionType.existing)
    
    def timed_out_ussd_session(self, msisdn, message):
        log.msg('%s timed out, removing client' % msisdn)
    
    def end_ussd_session(self, msisdn, message):
        log.msg('%s ended the session, removing client' % msisdn)
    


class USSDWorker(PubSubWorker):
    consumer_class = EchoConsumer
    publisher_class = Publisher
