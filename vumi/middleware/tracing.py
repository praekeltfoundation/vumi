# -*- test-case-name: vumi.middleware.tests.test_tracing -*-
import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet import reactor

from vumi.persist.txredis_manager import TxRedisManager
from vumi.config import Config, ConfigDict, ConfigInt
from vumi.middleware.base import BaseMiddleware


class BaseMiddlewareWithConfig(BaseMiddleware):

    CONFIG_CLASS = Config

    def __init__(self, name, config, worker):
        self.name = name
        self.config = self.CONFIG_CLASS(config, static=True)
        self.worker = worker


class TracingMiddlewareConfig(BaseMiddlewareWithConfig.CONFIG_CLASS):

    redis_manager = ConfigDict("Redis Config", static=True)
    lifetime = ConfigInt('How long to keep a trace for in seconds',
                         static=True, default=600)


def msg_key(message_id):
    return ':'.join(['trace', message_id])


class TraceManager(object):

    def __init__(self, redis):
        self.redis = redis

    def get_trace(self, message_id):
        d = self.redis.lrange(msg_key(message_id), 0, 10)
        d.addCallback(lambda hops: [json.loads(hop) for hop in hops])
        return d


class TracingMiddleware(BaseMiddlewareWithConfig):
    """
    Middleware for tracing all the hops and associated timestamps
    a message took when being routed through the system
    """
    CONFIG_CLASS = TracingMiddlewareConfig
    clock = reactor

    @inlineCallbacks
    def setup_middleware(self):
        self.redis = yield TxRedisManager.from_config(
            self.config.redis_manager)
        self.transport_name = self.worker.transport_name
        self.clock = self.get_clock()

    def get_clock(self):
        return self.clock

    def teardown_middleware(self):
        return self.redis.close()

    def store_hop(self, message_id, data):
        data.setdefault('time', self.clock.seconds())
        data.setdefault('transport_name', self.transport_name)
        key = msg_key(message_id)
        d = self.redis.rpush(key, json.dumps(data))
        d.addCallback(lambda _: self.redis.expire(key, self.config.lifetime))
        return d

    def store_msg_hop(self, message, connector_name, direction):
        d = self.store_hop(message['message_id'], {
            'message_id': message['message_id'],
            'message_type': message['message_type'],
            'direction': direction,
            'connector_name': connector_name,
            'content': message['content'],
        })
        d.addCallback(lambda _: message)
        return d

    def store_event_hop(self, event, connector_name):
        data = {
            'message_type': event['message_type'],
            'event_id': event['event_id'],
            'event_type': event['event_type'],
        }

        if data['event_type'] == 'nack':
            data['nack_reason'] = event['nack_reason']
        elif data['event_type'] == 'delivery_report':
            data['delivery_status'] = event['delivery_status']

        d = self.store_hop(event['user_message_id'], data)
        d.addCallback(lambda _: event)
        return d

    def handle_inbound(self, message, connector_name):
        return self.store_msg_hop(message, connector_name, 'inbound')

    def handle_outbound(self, message, connector_name):
        d = self.store_msg_hop(message, connector_name, 'outbound')
        d.addCallback(self._link_reply, connector_name)
        return d

    def _link_reply(self, message, connector_name):
        in_reply_to = message.get('in_reply_to')
        if in_reply_to is not None:
            return self.store_hop(in_reply_to, {
                'message_type': message['message_type'],
                'direction': 'outbound',
                'connector_name': connector_name,
                'reply_message_id': message['message_id'],
                'content': message['content'],
            })
        return message

    def handle_event(self, message, connector_name):
        return self.store_event_hop(message, connector_name)
