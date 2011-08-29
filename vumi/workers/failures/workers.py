# -*- test-case-name: vumi.workers.failures.tests.test_workers -*-

import time
import json
from datetime import datetime
from uuid import uuid4

import redis
from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.utils import get_deploy_int


class FailureWorker(Worker):
    """
    Base class for transport failure handlers.

    Subclasses should implement :meth:`handle_failure`.
    """

    GRANULARITY = 5  # seconds

    MAX_DELAY = 3600
    INITIAL_DELAY = 1
    DELAY_FACTOR = 3

    @inlineCallbacks
    def startWorker(self):
        # Connect to Redis
        if not hasattr(self, 'r_server'):
            self.r_server = redis.Redis(
                "localhost", db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = "failures:%s" % (self.config['transport_name'],)
        log.msg("r_prefix = %s" % self.r_prefix)
        self._retry_timestamps_key = self.r_key("retry_timestamps")
        retry_rkey = self.get_rkey('retry')
        failures_rkey = self.get_rkey('failures')
        self.retry_publisher = yield self.publish_to(retry_rkey)
        self.consumer = yield self.consume(failures_rkey, self.process_message)

    def get_rkey(self, route_name):
        return self.config['%s_routing_key' % route_name] % self.config

    def r_key(self, key):
        """
        Prefix ``key`` with a worker-specific string.
        """
        return "#".join((self.r_prefix, key))

    def failure_key(self, timestamp=None, failure_id=None):
        """
        Construct a failure key.
        """
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
        """
        Store this failure in redis, with an optional retry delay.

        :param message: The failed message.
        :param reason: A string containing the failure reason.
        :param retry_delay: The (optional) retry delay in seconds.

        If ``retry_delay`` is not ``None``, a retry will be scheduled
        approximately ``retry_delay`` seconds in the future.
        """
        message_json = message
        if not isinstance(message, basestring):
            # This isn't already JSON-encoded.
            message_json = json.dumps(message)
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
        publisher.publish_raw(failure['message'])

    def deliver_retries(self):
        while True:
            retry_key = self.get_next_retry_key()
            if not retry_key:
                return
            self.deliver_retry(retry_key, self.retry_publisher)

    def next_retry_delay(self, delay):
        if not delay:
            return self.INITIAL_DELAY
        return min(delay * self.DELAY_FACTOR, self.MAX_DELAY)

    def update_retry_metadata(self, message):
        rmd = message.get('retry_metadata', {})
        message['retry_metadata'] = {
            'retries': rmd.get('retries', 0) + 1,
            'delay': self.next_retry_delay(rmd.get('delay', 0)),
            }
        return message

    def handle_failure(self, message, reason):
        """
        Handle a failed message from a transport.

        :param message: The failed message, as a dict.
        :param reason: A string containing the reason for the failure.

        This method should be implemented in subclasses to handle
        failures specific to particular transports.
        """
        raise NotImplementedError()

    def process_message(self, failure_message):
        message = failure_message.payload['message']
        reason = failure_message.payload['reason']
        self.handle_failure(message, reason)
