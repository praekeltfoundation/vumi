# -*- test-case-name: vumi.middleware.tests.test_tracing -*-

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.persist.txredis_manager import TxRedisManager
from vumi.config import Config, ConfigDict
from vumi.middleware.base import BaseMiddleware


class BaseMiddlewareWithConfig(BaseMiddleware):

    CONFIG_CLASS = Config

    def __init__(self, name, config, worker):
        self.name = name
        self.config = self.CONFIG_CLASS(config, static=True)
        self.worker = worker


class TracingMiddlewareConfig(BaseMiddlewareWithConfig.CONFIG_CLASS):

    redis_manager = ConfigDict("Redis Config", static=True)


class TracingMiddleware(BaseMiddlewareWithConfig):
    """
    Middleware for tracing all the hops and associated timestamps
    a message took when being routed through the system
    """
    CONFIG_CLASS = TracingMiddlewareConfig

    @inlineCallbacks
    def setup_middleware(self):
        self.redis = yield TxRedisManager.from_config(
            self.config.redis_manager)

    def teardown_middleware(self):
        return self.redis.close()

    def handle_inbound(self, message, connector_name):
        pass

    def handle_outbound(self, message, connector_name):
        pass

    def handle_event(self, message, connector_name):
        pass
