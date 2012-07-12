# -*- test-case-name: vumi.transports.httprpc.tests.test_httprpc -*-

import json
import redis

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource

from vumi.transports.base import Transport


class SmsSyncHealthResource(Resource):
    isLeaf = True

    def render(self, request):
        request.setResponseCode(http.OK)
        request.do_not_log = True
        return 'OK'


class SmsSyncResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render(self, request):
        return 'bar'


class SmsSyncTransport(Transport):
    """
    Ushandi SMSSync Transport

    :param str web_path:
        The HTTP path to listen on.
    :param int web_port:
        The HTTP port
    :param str secret:
        An optional authentication method
    """

    def validate_config(self):
        self.web_path = self.config['web_path']
        self.web_port = int(self.config['web_port'])
        self.secret = self.config.get('secret', '')
        
        self.redis_config = self.config.get('redis_config', {})
        self.r_prefix = "smssync:%s" % (self.config['transport_name'],)

    @inlineCallbacks
    def setup_transport(self):
        self.web_resource = yield self.start_web_resources(
            [
                (SmsSyncResource(self), self.web_path),
                (SmsSyncHealthResource(), 'health')
            ], self.web_port
        )
        
        self.r_server = redis.Redis(**self.redis_config)
    
    def rkey_sms(self, direction):
        return "%s:%s" % (self.r_prefix, direction)

    def store_sms(self, sent_from, message, message_id, sent_to, sent_timestamp):
        rkey_sms = self.rkey_sms('incoming')
        payload = json.dumps({'sent_from': sent_from, 'message': message, 
                            'message_id': message_id, 'sent_to': sent_to, 
                            'sent_timestamp': sent_timestamp})
        self.r_server.rpush(rkey_sms, payload)

    def retrieve_smses(self):
        self.rkey_memo('outgoing')
        
