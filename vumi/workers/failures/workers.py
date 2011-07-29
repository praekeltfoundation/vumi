# -*- test-case-name: vumi.workers.failures.tests.test_workers -*-

from datetime import datetime
from uuid import uuid4

import redis
from twisted.python import log

from vumi.service import Worker
from vumi.utils import get_deploy_int


class Vas2NetsFailureWorker(Worker):
    """
    Handler for transport failures in the Vas2Nets transport.
    """

    def startWorker(self):
        # Connect to Redis
        self.r_server = redis.Redis("localhost",
                                    db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = ":".join(["failures", self.config['transport_name']])
        log.msg("r_prefix = %s" % self.r_prefix)

    def r_key(self, key):
        return "#".join((self.r_prefix, key))

    def r_set(self, key, value):
        self.r_server.set(self.r_key(key), value)

    def r_get(self, key):
        return self.r_server.get(self.r_key(key))

    def failure_key(self, timestamp=None, failure_id=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        if failure_id is None:
            failure_id = uuid4().get_hex()
        timestamp = timestamp.isoformat().split('.')[0]
        return self.r_key(".".join(("failure", timestamp, failure_id)))

    def add_to_failure_set(self, key):
        self.r_server.sadd(self.r_key("failure_keys"), key)

    def get_failure_keys(self):
        return self.r_server.smembers(self.r_key("failure_keys"))

    def store_failure(self, message, reason, retry=False):
        key = self.failure_key()
        self.r_server.hmset(key, {
                "message": message,
                "reason": reason,
                "retry": retry,
                })
        self.add_to_failure_set(key)
        return key

