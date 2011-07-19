# -*- test-case-name: vumi.workers.vas2nets.test_vas2nets -*-
# -*- encoding: utf-8 -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, Deferred

from vumi.message import Message
from vumi.service import Worker


class EchoWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        """called by the Worker class when the AMQP connections been established"""
        self.publisher = yield self.publish_to('sms.outbound.%(transport_name)s' % self.config)
        self.consumer = yield self.consume('sms.inbound.%(transport_name)s.%(shortcode)s' % self.config,
                                           self.handle_inbound_message)

    def handle_inbound_message(self, message):
        log.msg("Received: %s" % (message.payload,))
        """Reply to the message with the same content"""
        data = message.payload

        reply = {
            'to_msisdn': data['from_msisdn'],
            'from_msisdn': data['to_msisdn'],
            'message': data['message'],
            'id': data['transport_message_id'],
            'transport_network_id': data['transport_network_id'],
            }

        return self.publisher.publish_message(Message(**reply))

    def stopWorker(self):
        """shutdown"""
        pass
