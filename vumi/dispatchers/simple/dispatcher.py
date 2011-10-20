
from twisted.python import log

from vumi.message import Message, TransportUserMessage
from vumi.service import Worker


class SimpleDispatcher(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting SimpleDispatcher with config: %s" % (self.config))

        #self.publisher = yield self.publish_to(self.publish_key)
        #self.consume(self.consume_key, self.consume_message)

    def consume_message(self, message):
        log.msg("SimpleDispatcher consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        user_m = TransportUserMessage(**message.payload)
        request = user_m.payload['content']
        msisdn = user_m.payload['from_addr']
        session_event = user_m.payload.get('session_event')
        provider = user_m.payload.get('provider')
        def response_callback(resp):
            reply = user_m.reply(str(resp))
            self.publisher.publish_message(reply)
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.ds,
                msisdn,
                session_event,
                provider,
                request,
                response_callback)

    def stopWorker(self):
        log.msg("Stopping the IkhweziQuizWorker")
