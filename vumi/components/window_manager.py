# -*- test-case-name: vumi.components.tests.test_window_manager -*-
import json
import uuid

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall

from vumi import log


class WindowException(Exception):
    pass


class WindowManager(object):

    WINDOW_KEY = 'windows'
    FLIGHT_KEY = 'inflight'
    FLIGHT_STATS_KEY = 'flightstats'
    MAP_KEY = 'keymap'

    def __init__(self, redis, window_size=100, flight_lifetime=None,
                gc_interval=10):
        self.window_size = window_size
        self.flight_lifetime = flight_lifetime or (gc_interval * window_size)
        self.redis = redis
        self.clock = self.get_clock()
        self.gc = LoopingCall(self.clear_expired_flight_keys)
        self.gc.clock = self.clock
        self.gc.start(gc_interval)
        self._monitor = None

    def noop(self, *args, **kwargs):
        pass

    def stop(self):
        if self._monitor and self._monitor.running:
            self._monitor.stop()

        if self.gc.running:
            self.gc.stop()

    def get_windows(self):
        return self.redis.zrange(self.window_key(), 0, -1)

    @inlineCallbacks
    def window_exists(self, window_id):
        score = yield self.redis.zscore(self.window_key(), window_id)
        if score is not None:
            returnValue(True)
        returnValue(False)

    def window_key(self, *keys):
        return ':'.join([self.WINDOW_KEY] + map(unicode, keys))

    def flight_key(self, *keys):
        return self.window_key(self.FLIGHT_KEY, *keys)

    def stats_key(self, *keys):
        return self.window_key(self.FLIGHT_STATS_KEY, *keys)

    def map_key(self, *keys):
        return self.window_key(self.MAP_KEY, *keys)

    def get_clock(self):
        return reactor

    def get_clocktime(self):
        return self.clock.seconds()

    @inlineCallbacks
    def create_window(self, window_id, strict=False):
        if strict and (yield self.window_exists(window_id)):
            raise WindowException('Window already exists: %s' % (window_id,))
        clock_time = self.get_clocktime()
        yield self.redis.zadd(self.WINDOW_KEY, **{
            window_id: clock_time,
            })
        returnValue(clock_time)

    @inlineCallbacks
    def remove_window(self, window_id):
        waiting_list = yield self.count_waiting(window_id)
        if waiting_list:
            raise WindowException('Window not empty')
        yield self.redis.zrem(self.WINDOW_KEY, window_id)

    @inlineCallbacks
    def add(self, window_id, data, key=None):
        key = key or uuid.uuid4().get_hex()
        # The redis.set() has to complete before redis.lpush(),
        # otherwise the key can be popped from the window before the
        # data is available.
        yield self.redis.set(self.window_key(window_id, key),
                             json.dumps(data))
        yield self.redis.lpush(self.window_key(window_id), key)
        returnValue(key)

    @inlineCallbacks
    def get_next_key(self, window_id):

        window_key = self.window_key(window_id)
        inflight_key = self.flight_key(window_id)

        waiting_list = yield self.count_waiting(window_id)
        if waiting_list == 0:
            return

        flight_size = yield self.count_in_flight(window_id)
        room_available = self.window_size - flight_size

        if room_available > 0:
            log.debug('Window %s has space for %s' % (window_key,
                                                        room_available))
            next_key = yield self.redis.rpoplpush(window_key, inflight_key)
            if next_key:
                yield self._set_timestamp(window_id, next_key)
                returnValue(next_key)

    def _set_timestamp(self, window_id, flight_key):
        return self.redis.zadd(self.stats_key(window_id), **{
                flight_key: self.get_clocktime(),
        })

    def _clear_timestamp(self, window_id, flight_key):
        return self.redis.zrem(self.stats_key(window_id), flight_key)

    def count_waiting(self, window_id):
        window_key = self.window_key(window_id)
        return self.redis.llen(window_key)

    def count_in_flight(self, window_id):
        flight_key = self.flight_key(window_id)
        return self.redis.llen(flight_key)

    def get_expired_flight_keys(self, window_id):
        return self.redis.zrangebyscore(self.stats_key(window_id),
            '-inf', self.get_clocktime() - self.flight_lifetime)

    @inlineCallbacks
    def clear_expired_flight_keys(self):
        windows = yield self.get_windows()
        for window_id in windows:
            expired_keys = yield self.get_expired_flight_keys(window_id)
            for key in expired_keys:
                yield self.redis.lrem(self.flight_key(window_id), key, 1)

    @inlineCallbacks
    def get_data(self, window_id, key):
        json_data = yield self.redis.get(self.window_key(window_id, key))
        returnValue(json.loads(json_data))

    @inlineCallbacks
    def remove_key(self, window_id, key):
        yield self.redis.lrem(self.flight_key(window_id), key, 1)
        yield self.redis.delete(self.window_key(window_id, key))
        yield self.redis.delete(self.stats_key(window_id, key))
        yield self.clear_external_id(window_id, key)
        yield self._clear_timestamp(window_id, key)

    @inlineCallbacks
    def set_external_id(self, window_id, flight_key, external_id):
        yield self.redis.set(self.map_key(window_id, 'internal', external_id),
            flight_key)
        yield self.redis.set(self.map_key(window_id, 'external', flight_key),
            external_id)

    def get_internal_id(self, window_id, external_id):
        return self.redis.get(self.map_key(window_id, 'internal', external_id))

    def get_external_id(self, window_id, flight_key):
        return self.redis.get(self.map_key(window_id, 'external', flight_key))

    @inlineCallbacks
    def clear_external_id(self, window_id, flight_key):
        external_id = yield self.get_external_id(window_id, flight_key)
        if external_id:
            yield self.redis.delete(self.map_key(window_id, 'external',
                                                 flight_key))
            yield self.redis.delete(self.map_key(window_id, 'internal',
                                                 external_id))

    def monitor(self, key_callback, interval=10, cleanup=True,
                cleanup_callback=None):

        if self._monitor is not None:
            raise WindowException('Monitor already started')

        self._monitor = LoopingCall(lambda: self._monitor_windows(
            key_callback, cleanup, cleanup_callback))
        self._monitor.clock = self.get_clock()
        self._monitor.start(interval)

    @inlineCallbacks
    def _monitor_windows(self, key_callback, cleanup=True,
                         cleanup_callback=None):
        windows = yield self.get_windows()
        for window_id in windows:
            key = (yield self.get_next_key(window_id))
            while key:
                yield key_callback(window_id, key)
                key = (yield self.get_next_key(window_id))

            # Remove empty windows if required
            if cleanup and not ((yield self.count_waiting(window_id)) or
                                (yield self.count_in_flight(window_id))):
                if cleanup_callback:
                    cleanup_callback(window_id)
                yield self.remove_window(window_id)
