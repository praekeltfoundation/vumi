import json
import redis

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker, Consumer, Publisher
from vumi.session import getVumiSession, delVumiSession, TraversedDecisionTree
from vumi.utils import get_deploy_int


class SessionConsumer(Consumer):
    vhost = "/develop"
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "vumi.inbound.session.default"  # TODO revise name
    routing_key = "vumi.inbound.session.default"  # TODO revise name
    yaml_template = None
    data_url = {"username": None, "password": None, "url": None, "params": []}
    post_url = {"username": None, "password": None, "url": None, "params": []}

    def __init__(self, publisher):
        self.publisher = publisher
        if hasattr(publisher, 'vumi_options'):
            self.vhost = publisher.vumi_options.get('vhost', self.vhost)
        self.r_server = redis.Redis("localhost", db=get_deploy_int(self.vhost))

    def set_yaml_template(self, yaml_template):
        self.yaml_template = yaml_template

    def set_data_url(self, data_source):
        self.data_url = data_source

    def set_post_url(self, post_source):
        self.post_url = post_source

    def consume_message(self, message):
        # TODO: Eep! This code is broken!
        log.msg("session message %s consumed by %s" % (
            json.dumps(dictionary), self.__class__.__name__))
        #dictionary = message.get('short_message')

    def get_session(self, MSISDN):
        sess = getVumiSession(self.r_server,
                              self.routing_key + '.' + MSISDN)
        if not sess.get_decision_tree():
            sess.set_decision_tree(self.setup_new_decision_tree(MSISDN))
        return sess

    def del_session(self, MSISDN):
        return delVumiSession(self.r_server, MSISDN)

    def setup_new_decision_tree(self, MSISDN, **kwargs):
        decision_tree = TraversedDecisionTree()
        yaml_template = self.yaml_template
        decision_tree.load_yaml_template(yaml_template)
        self.set_data_url(decision_tree.get_data_source())
        self.set_post_url(decision_tree.get_post_source())
        if self.data_url.get('url'):
            raise ValueError("This is broken. Sorry. :-(")
        else:
            decision_tree.load_dummy_data()
        return decision_tree


class SessionPublisher(Publisher):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "vumi.outbound.session.fallback"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s"
                % (message, kwargs))
        super(SessionPublisher, self).publish_message(message, **kwargs)


class SessionWorker(Worker):
    """
    A worker that runs a set statefull interactive sessions
    with multiple MSISDN's
    """

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SessionWorker")
        self.publisher = yield self.start_publisher(SessionPublisher)
        yield self.start_consumer(SessionConsumer, self.publisher)

    def stopWorker(self):
        log.msg("Stopping the SessionWorker")
