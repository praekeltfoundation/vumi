# -*- test-case-name: vumi.middleware.tests.test_provider_setter -*-

from twisted.internet.defer import inlineCallbacks

from vumi.middleware.base import BaseMiddleware
from vumi.persist.txredis_manager import TxRedisManager


class SessionLengthMiddleware(BaseMiddleware):
    """ Middleware for storing the session length in the message.

    Session length is stored if the end of the session is reached.

    Configuration option:

    :param dict redis:
        Redis configuration parameters.
    """
    @inlineCallbacks
    def setup_middleware(self):
        r_config = self.config.get('redis_manager', {})
        self.redis = yield TxRedisManager.from_config(r_config)

    @inlineCallbacks
    def teardown_middleware(self):
        yield self.redis.close_manager()
