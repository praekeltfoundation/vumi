from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.service import Worker, Consumer, Publisher
from vumi.session import VumiSession
from vumi.message import Message, VUMI_DATE_FORMAT

from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

class MenuConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "vumi.inbound.session.default" #TODO revise name
    routing_key = "vumi.inbound.session.default" #TODO revise name
    sessions = {}


    def __init__(self, publisher):
        self.publisher = publisher


    def consume_message(self, message):
        log.msg("menu message %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
        #dictionary = message.get('short_message')


    def call_for_json(self, MSISDN, user_name):
        if 1 == 2:
            pass
        else:
            log.msg("OMG the interwebs are broken!")
            return None


    def get_session(self, MSISDN):
        session = self.sessions.get(MSISDN)
        if not session:
            session = VumiSession()
            self.sessions[MSISDN] = session
        return session


class MenuPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "vumi.outbound.session.fallback"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s" % (message, kwargs))
        super(MenuPublisher, self).publish_message(message, **kwargs)

