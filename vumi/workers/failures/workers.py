# -*- test-case-name: vumi.workers.failures.tests.test_workers -*-

import time
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

    GRANULARITY = 5 # seconds

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

    def store_failure(self, message, reason, retry_delay=None):
        key = self.failure_key()
        self.r_server.hmset(key, {
                "message": message,
                "reason": reason,
                "retry_delay": retry_delay,
                })
        self.add_to_failure_set(key)
        if retry_delay is not None:
            self.store_retry(key, retry_delay)
        return key

    def store_retry(self, failure_key, retry_delay, now=None):
        timestamp = self.get_next_write_timestamp(retry_delay, now=now)
        bucket_key = self.r_key("retry_keys." + timestamp)
        self.r_server.sadd(bucket_key, failure_key)
        self.store_read_timestamp(timestamp)

    def store_read_timestamp(self, timestamp):
        score = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S"))
        self.r_server.zadd(self.r_key("retry_timestamps"), **{timestamp: score})

    def get_next_write_timestamp(self, delta, now=None):
        if now is None:
            now = int(time.time())
        timestamp = now + delta
        timestamp += self.GRANULARITY - (timestamp % self.GRANULARITY)
        return datetime.utcfromtimestamp(timestamp).isoformat().split('.')[0]

    def get_next_read_timestamp(self, now=None):
        if now is None:
            now = int(time.time())
        timestamp = datetime.utcfromtimestamp(now).isoformat().split('.')[0]
        next_timestamp = self.r_server.zrange(self.r_key("retry_timestamps"), 0, 0)
        if next_timestamp and next_timestamp[0] <= timestamp:
            return next_timestamp[0]
        return None
