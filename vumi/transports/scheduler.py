# -*- test-case-name: vumi.transports.tests.test_scheduler -*-
import time
import iso8601
import pytz
import json
from datetime import datetime
from uuid import uuid4
import warnings

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import LoopingCall

from vumi import message


warnings.warn("vumi.transport.scheduler is deprecated. A replacement is coming"
              " soon.", category=DeprecationWarning)


class Scheduler(object):
    """
    Base class for stuff that needs to be published to a given queue
    at a given time.
    """

    def __init__(self, redis, callback, prefix='scheduler',
                    granularity=5, delivery_period=3, json_encoder=None,
                    json_decoder=None):
        self.r_server = redis
        self.r_prefix = prefix
        self.granularity = granularity
        self.delivery_period = delivery_period
        self._scheduled_timestamps_key = self.r_key("scheduled_timestamps")
        self.callback = callback
        self.json_encoder = json_encoder or message.JSONMessageEncoder
        self.json_decoder = json_decoder or message.date_time_decoder
        self.loop = LoopingCall(self.deliver_scheduled)

    @property
    def is_running(self):
        return self.loop.running

    def start(self):
        if not self.loop.running:
            self.loop.start(self.delivery_period, now=True)

    def stop(self):
        if self.loop.running:
            self.loop.stop()

    def r_key(self, key):
        """
        Prefix ``key`` with a worker-specific string.
        """
        return "#".join((self.r_prefix, key))

    def scheduled_key(self):
        """
        Construct a unique scheduled key.
        """
        timestamp = datetime.utcnow()
        unique_id = uuid4().get_hex()
        timestamp = timestamp.isoformat().split('.')[0]
        return self.r_key(".".join(("scheduled", timestamp, unique_id)))

    def get_scheduled(self, scheduled_key):
        return self.r_server.hgetall(scheduled_key)

    def get_next_write_timestamp(self, delta, now):
        now = int(now)
        return self.get_time_bucket(now + delta)

    def get_time_bucket(self, timestamp):
        timestamp += self.granularity - (timestamp % self.granularity)
        return datetime.utcfromtimestamp(timestamp).isoformat().split('.')[0]

    def get_read_timestamp(self, now):
        now = int(now)
        timestamp = datetime.utcfromtimestamp(now).replace(tzinfo=pytz.UTC)
        next_timestamp = self.r_server.zrange(self._scheduled_timestamps_key,
                                                0, 0)
        if next_timestamp:
            if iso8601.parse_date(next_timestamp[0]) <= timestamp:
                return next_timestamp[0]
        return None

    def get_next_read_timestamp(self):
        return self.get_read_timestamp(time.time())

    def get_scheduled_key(self, time):
        timestamp = self.get_time_bucket(time)
        bucket_key = self.r_key("scheduled_keys." + timestamp)
        # key of message to be delivered
        scheduled_key = self.r_server.spop(bucket_key)
        # if the set is empty, remove the timestamp entry from the
        # scheduled timestamps key
        if self.r_server.scard(bucket_key) < 1:
            self.r_server.zrem(self._scheduled_timestamps_key, timestamp)
        return scheduled_key

    def schedule(self, delta, payload, now=None):
        """
        Store the payload in Redis and call `self.callback` after
        `delta` seconds as counted from `now` onwards.


        :param delta: the amount of seconds
        :param payload: the payload send to `self.callback`
        :param now: Used to calculate the delta (timestamp in
                    seconds since epoch)

        If ``now`` is ``None`` then it will default to ``time.time()``
        """
        # do this first as we want it to blow up before any keys
        # are set should the content not be JSON encodable
        if not now:
            now = int(time.time())

        key = self.scheduled_key()
        self.add_to_scheduled_set(key)
        bucket_key = self.store_scheduled(key, delta, now)
        self.r_server.hmset(key, {
            'payload': json.dumps(payload, cls=self.json_encoder),
            'scheduled_at': datetime.utcnow().isoformat(),
            'bucket_key': bucket_key,
        })
        return key, bucket_key

    def add_to_scheduled_set(self, key):
        self.r_server.sadd(self.r_key("scheduled_keys"), key)

    def store_scheduled(self, scheduled_key, delta, now):
        timestamp = self.get_next_write_timestamp(delta, now)
        bucket_key = self.r_key("scheduled_keys." + timestamp)
        self.r_server.sadd(bucket_key, scheduled_key)
        self.store_read_timestamp(timestamp)
        return bucket_key

    def store_read_timestamp(self, timestamp):
        score = time.mktime(time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S"))
        self.r_server.zadd(self._scheduled_timestamps_key, **{
            timestamp: score
        })

    def get_all_scheduled_keys(self):
        return self.r_server.smembers(self.r_key("scheduled_keys"))

    @inlineCallbacks
    def deliver_scheduled(self, _time=None):
        _time = _time or int(time.time())
        while True:
            scheduled_key = self.get_scheduled_key(_time - self.granularity)
            if not scheduled_key:
                return
            scheduled_data = self.get_scheduled(scheduled_key)
            scheduled_at = scheduled_data['scheduled_at']
            payload = json.loads(scheduled_data['payload'],
                                    object_hook=self.json_decoder)
            yield self.callback(scheduled_at, payload)
            self.clear_scheduled(scheduled_key)

    def clear_scheduled(self, key):
        self.r_server.srem(self.r_key("scheduled_keys"), key)
        message_data = self.get_scheduled(key)
        bucket_key = message_data['bucket_key']
        self.r_server.srem(bucket_key, key)
        self.r_server.delete(key)
