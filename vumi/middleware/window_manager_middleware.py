from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from vumi.middleware import BaseMiddleware
from vumi.message import TransportUserMessage
from vumi.persist.txredis_manager import TxRedisManager

from vumi.components.window_manager import WindowManager


class WindowManagerMiddleware(BaseMiddleware):

    @inlineCallbacks
    def setup_middleware(self):
        store_prefix = self.config.get('store_prefix', 'message_store')
        r_config = self.config.get('redis_manager', {})
        redis = yield TxRedisManager.from_config(r_config)

        self.transport_name = self.worker.transport_name

        self.wm = WindowManager(
            redis,
            window_size=self.config['window_size'],
            flight_lifetime=self.config['flight_lifetime'])

        self.wm.monitor(
            self.send_outbound,
            self.config['monitor_loop'],
            False)

        if not (yield self.wm.window_exists(self.transport_name)):
            yield self.wm.create_window(self.transport_name)

    def teardown_middleware(self):
        self.wm.stop()

    @inlineCallbacks
    def handle_event(self, event, endpoint):
        if event["event_type"] in ['ack', 'nack']:
            yield self.wm.remove_key(
                self.transport_name,
                event['user_message_id'])
        returnValue(event)

    @inlineCallbacks
    def handle_outbound(self, msg, endpoint):
        yield self.wm.add(self.transport_name, msg.to_json(), msg["message_id"])
        #TODO: should be replaced by a StopPropagation mechanism
        returnValue(None)

    @inlineCallbacks
    def send_outbound(self, window_id, key):
        data = yield self.wm.get_data(window_id, key)
        msg = TransportUserMessage.from_json(data)
        self.worker.handle_outbound_message(msg)
