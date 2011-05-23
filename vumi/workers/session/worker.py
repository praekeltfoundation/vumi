import json
import redis
import yaml

from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.service import Worker, Consumer, Publisher
from vumi.session import getVumiSession, delVumiSession, TraversedDecisionTree
from vumi.message import Message, VUMI_DATE_FORMAT
from vumi.webapp.api import utils

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
    yaml_template = None
    data_url = {"username":None, "password":None, "url":None, "params":[]}
    post_url = {"username":None, "password":None, "url":None, "params":[]}


    def __init__(self, publisher):
        self.publisher = publisher
        self.r_server = redis.Redis("localhost")

    def set_yaml_template(self, yaml_template):
        self.yaml_template = yaml_template

    def set_data_url(self, data_source):
        self.data_url = data_source

    def set_post_url(self, post_source):
        self.post_url = post_source


    def consume_message(self, message):
        log.msg("session message %s consumed by %s" % (
            json.dumps(dictionary),self.__class__.__name__))
        #dictionary = message.get('short_message')


    def call_for_json(self, MSISDN):
        if self.data_url['url']:
            params = [(self.data_url['params'][0], str(MSISDN))]
            url = self.data_url['url']
            auth_string = ''
            if self.data_url['username']:
                auth_string += self.data_url['username']
                if self.data_url['password']:
                    auth_string += ":" + self.data_url['password']
                auth_string += "@"
            resp_url, resp = utils.callback("http://"+auth_string+url, params)
            return resp
        return None


    def post_back_json(self, MSISDN):
        session = self.getVumiSession(self.r_server, MSISDN)
        if session and session.get_decision_tree():
            json_string = json.dumps(session.get_decision_tree().get_data())
            if self.post_url['url']:
                params = [(self.post_url['params'][0], json_string)]
                url = self.post_url['url']
                auth_string = ''
                if self.post_url['username']:
                    auth_string += self.post_url['username']
                    if self.post_url['password']:
                        auth_string += ":" + self.post_url['password']
                    auth_string += "@"
                resp_url, resp = utils.callback("http://"+auth_string+url, params)
                return resp
        return None


    def get_session(self, MSISDN):
        sess = getVumiSession(self.r_server, MSISDN)
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
            json_data = self.call_for_json(MSISDN)
            decision_tree.load_json_data(json_data)
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
        log.msg("Publishing Message %s with extra args: %s" % (message, kwargs))
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

