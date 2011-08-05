# -*- test-case-name: vumi.workers.failures.tests.test_workers -*-

import time
import json
from datetime import datetime
from uuid import uuid4

import redis
from twisted.python import log

from vumi.service import Worker
from vumi.utils import get_deploy_int


class FailureWorker(Worker):
    """
    Base class for transport failure handlers.
    """

    GRANULARITY = 5  # seconds

    def startWorker(self):
        # Connect to Redis
        self.r_server = redis.Redis("localhost",
                                    db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = ":".join(["failures", self.config['transport_name']])
        log.msg("r_prefix = %s" % self.r_prefix)
        self._retry_timestamps_key = self.r_key("retry_timestamps")

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

    def store_failure(self, message_json, reason, retry_delay=None):
        key = self.failure_key()
        if not retry_delay:
            retry_delay = 0
        self.r_server.hmset(key, {
                "message": message_json,
                "reason": reason,
                "retry_delay": str(retry_delay),
                })
        self.add_to_failure_set(key)
        if retry_delay:
            self.store_retry(key, retry_delay)
        return key

    def get_failure(self, failure_key):
        return self.r_server.hgetall(failure_key)

    def store_retry(self, failure_key, retry_delay, now=None):
        timestamp = self.get_next_write_timestamp(retry_delay, now=now)
        bucket_key = self.r_key("retry_keys." + timestamp)
        self.r_server.sadd(bucket_key, failure_key)
        self.store_read_timestamp(timestamp)

    def store_read_timestamp(self, timestamp):
        score = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S"))
        self.r_server.zadd(self._retry_timestamps_key, **{timestamp: score})

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
        next_timestamp = self.r_server.zrange(self._retry_timestamps_key, 0, 0)
        if next_timestamp and next_timestamp[0] <= timestamp:
            return next_timestamp[0]
        return None

    def get_next_retry_key(self, now=None):
        timestamp = self.get_next_read_timestamp(now)
        if not timestamp:
            return None
        bucket_key = self.r_key("retry_keys." + timestamp)
        failure_key = self.r_server.spop(bucket_key)
        if self.r_server.scard(bucket_key) < 1:
            self.r_server.zrem(self._retry_timestamps_key, timestamp)
        return failure_key

    def deliver_retry(self, retry_key, publisher):
        failure = self.get_failure(retry_key)
        # This needs to use Publisher.publish_raw() as soon as it
        # arrives from the metrics branch.
        publisher.publish_json(json.loads(failure['message']))

    def deliver_retries(self):
        # This assumes we have a suitable retry publisher. If not, it
        # will crash horribly.
        publisher = self.retry_publisher
        while True:
            retry_key = self.get_next_retry_key()
            if not retry_key:
                return
            self.deliver_retry(retry_key, publisher)

    def handle_failure(self, message, reason):
        raise NotImplementedError()

    def process_message(self, failure_message):
        message = json.dumps(failure_message.payload['message'])
        reason = failure_message.payload['reason']
        self.handle_failure(message, reason)


class Vas2NetsFailureWorker(FailureWorker):
    """
    Handler for transport failures in the Vas2Nets transport.
    """

    def handle_failure(self, message, reason):
        self.store_failure(message, reason)
