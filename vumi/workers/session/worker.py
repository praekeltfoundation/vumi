from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.service import Worker, Consumer, Publisher
from vumi.session import VumiSession, TraversedDecisionTree
from vumi.message import Message, VUMI_DATE_FORMAT

from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

class SessionConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "vumi.inbound.session.default" #TODO revise name
    routing_key = "vumi.inbound.session.default" #TODO revise name
    sessions = {}
    yaml_template


    def __init__(self, publisher):
        self.publisher = publisher


    def consume_message(self, message):
        log.msg("session message %s consumed by %s" % (json.dumps(dictionary),self.__class__.__name__))
        #dictionary = message.get('short_message')


    def call_for_json(self, MSISDN):
        return None


    def get_session(self, MSISDN):
        session = self.sessions.get(MSISDN)
        if not session:
            self.sessions[MSISDN] = self.create_new_session(MSISDN)
        return session


    def create_new_session(self, MSISDN, **kwargs):
            session = VumiSession()
            decision_tree = TraversedDecisionTree()
            session.set_decision_tree(decision_tree)
            yaml_template = self.yaml_template
            decision_tree.load_yaml_template(yaml_template)
            json_data = call_for_json(MSISDN)
            decision_tree.load_json_data(json_data)
            return session



class SessionPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "vumi.outbound.session.fallback"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s" % (message, kwargs))
        super(SessionPublisher, self).publish_message(message, **kwargs)


class SessionWorker(Worker):
    """
    A worker that breaks up batches of sms's into individual sms's
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SessionWorker")
        self.publisher = yield self.start_publisher(sessionPublisher)
        yield self.start_consumer(SessionConsumer, self.publisher)

    def stopWorker(self):
        log.msg("Stopping the SessionWorker")

