from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock

from vumi.components.window_manager import WindowManager, WindowException
from vumi.tests.helpers import VumiTestCase, PersistenceHelper


class TestWindowManager(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        redis = yield self.persistence_helper.get_redis_manager()
        self.window_id = 'window_id'

        # Patch the clock so we can control time
        self.clock = Clock()
        self.patch(WindowManager, 'get_clock', lambda _: self.clock)

        self.wm = WindowManager(redis, window_size=10, flight_lifetime=10)
        self.add_cleanup(self.wm.stop)
        yield self.wm.create_window(self.window_id)
        self.redis = self.wm.redis

    @inlineCallbacks
    def test_windows(self):
        windows = yield self.wm.get_windows()
        self.assertTrue(self.window_id in windows)

    def test_strict_window_recreation(self):
        return self.assertFailure(
            self.wm.create_window(self.window_id, strict=True),
                                    WindowException)

    @inlineCallbacks
    def test_window_recreation(self):
        orig_clock_time = self.clock.seconds()
        clock_time = yield self.wm.create_window(self.window_id)
        self.assertEqual(clock_time, orig_clock_time)

    @inlineCallbacks
    def test_window_removal(self):
        yield self.wm.add(self.window_id, 1)
        yield self.assertFailure(self.wm.remove_window(self.window_id),
            WindowException)
        key = yield self.wm.get_next_key(self.window_id)
        item = yield self.wm.get_data(self.window_id, key)
        self.assertEqual(item, 1)
        self.assertEqual((yield self.wm.remove_window(self.window_id)), None)

    @inlineCallbacks
    def test_adding_to_window(self):
        for i in range(10):
            yield self.wm.add(self.window_id, i)
        window_key = self.wm.window_key(self.window_id)
        window_members = yield self.redis.llen(window_key)
        self.assertEqual(window_members, 10)

    @inlineCallbacks
    def test_fetching_from_window(self):
        for i in range(12):
            yield self.wm.add(self.window_id, i)

        flight_keys = []
        for i in range(10):
            flight_key = yield self.wm.get_next_key(self.window_id)
            self.assertTrue(flight_key)
            flight_keys.append(flight_key)

        out_of_window_flight = yield self.wm.get_next_key(self.window_id)
        self.assertEqual(out_of_window_flight, None)

        # We should get data out in the order we put it in
        for i, flight_key in enumerate(flight_keys):
            data = yield self.wm.get_data(self.window_id, flight_key)
            self.assertEqual(data, i)

        # Removing one should allow for space for the next to fill up
        yield self.wm.remove_key(self.window_id, flight_keys[0])
        next_flight_key = yield self.wm.get_next_key(self.window_id)
        self.assertTrue(next_flight_key)

    @inlineCallbacks
    def test_set_and_external_id(self):
        yield self.wm.set_external_id(self.window_id, "flight_key",
                                      "external_id")
        self.assertEqual(
            (yield self.wm.get_external_id(self.window_id, "flight_key")),
            "external_id")
        self.assertEqual(
            (yield self.wm.get_internal_id(self.window_id, "external_id")),
            "flight_key")

    @inlineCallbacks
    def test_remove_key_removes_external_and_internal_id(self):
        yield self.wm.set_external_id(self.window_id, "flight_key",
                                      "external_id")
        yield self.wm.remove_key(self.window_id, "flight_key")
        self.assertEqual(
            (yield self.wm.get_external_id(self.window_id, "flight_key")),
            None)
        self.assertEqual(
            (yield self.wm.get_internal_id(self.window_id, "external_id")),
            None)

    @inlineCallbacks
    def assert_count_waiting(self, window_id, amount):
        self.assertEqual((yield self.wm.count_waiting(window_id)), amount)

    @inlineCallbacks
    def assert_expired_keys(self, window_id, amount):
        # Stuff has taken too long and so we should get 10 expired keys
        expired_keys = yield self.wm.get_expired_flight_keys(window_id)
        self.assertEqual(len(expired_keys), amount)

    @inlineCallbacks
    def assert_in_flight(self, window_id, amount):
        self.assertEqual((yield self.wm.count_in_flight(window_id)),
            amount)

    @inlineCallbacks
    def slide_window(self, limit=10):
        for i in range(limit):
            yield self.wm.get_next_key(self.window_id)

    @inlineCallbacks
    def test_expiry_of_acks(self):

        def mock_clock_time(self):
            return self._clocktime

        self.patch(WindowManager, 'get_clocktime', mock_clock_time)
        self.wm._clocktime = 0

        for i in range(30):
            yield self.wm.add(self.window_id, i)

        # We're manually setting the clock instead of using clock.advance()
        # so we can wait for the deferreds to finish before continuing to the
        # next clear_expired_flight_keys run since LoopingCall() will only fire
        # again if the previous run has completed.
        yield self.slide_window()
        self.wm._clocktime = 10
        yield self.wm.clear_expired_flight_keys()
        self.assert_expired_keys(self.window_id, 10)

        yield self.slide_window()
        self.wm._clocktime = 20
        yield self.wm.clear_expired_flight_keys()
        self.assert_expired_keys(self.window_id, 20)

        yield self.slide_window()
        self.wm._clocktime = 30
        yield self.wm.clear_expired_flight_keys()
        self.assert_expired_keys(self.window_id, 30)

        self.assert_in_flight(self.window_id, 0)
        self.assert_count_waiting(self.window_id, 0)

    @inlineCallbacks
    def test_monitor_windows(self):
        yield self.wm.remove_window(self.window_id)

        window_ids = ['window_id_1', 'window_id_2']
        for window_id in window_ids:
            yield self.wm.create_window(window_id)
            for i in range(20):
                yield self.wm.add(window_id, i)

        key_callbacks = {}

        def callback(window_id, key):
            key_callbacks.setdefault(window_id, []).append(key)

        cleanup_callbacks = []

        def cleanup_callback(window_id):
            cleanup_callbacks.append(window_id)

        yield self.wm._monitor_windows(callback, False)

        self.assertEqual(set(key_callbacks.keys()), set(window_ids))
        self.assertEqual(len(key_callbacks.values()[0]), 10)
        self.assertEqual(len(key_callbacks.values()[1]), 10)

        yield self.wm._monitor_windows(callback, False)

        # Nothing should've changed since we haven't removed anything.
        self.assertEqual(len(key_callbacks.values()[0]), 10)
        self.assertEqual(len(key_callbacks.values()[1]), 10)

        for window_id, keys in key_callbacks.items():
            for key in keys:
                yield self.wm.remove_key(window_id, key)

        yield self.wm._monitor_windows(callback, False)
        # Everything should've been processed now
        self.assertEqual(len(key_callbacks.values()[0]), 20)
        self.assertEqual(len(key_callbacks.values()[1]), 20)

        # Now run again but cleanup the empty windows
        self.assertEqual(set((yield self.wm.get_windows())), set(window_ids))
        for window_id, keys in key_callbacks.items():
            for key in keys:
                yield self.wm.remove_key(window_id, key)

        yield self.wm._monitor_windows(callback, True, cleanup_callback)
        self.assertEqual(len(key_callbacks.values()[0]), 20)
        self.assertEqual(len(key_callbacks.values()[1]), 20)
        self.assertEqual((yield self.wm.get_windows()), [])
        self.assertEqual(set(cleanup_callbacks), set(window_ids))


class TestConcurrentWindowManager(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        redis = yield self.persistence_helper.get_redis_manager()
        self.window_id = 'window_id'

        # Patch the count_waiting so we can fake the race condition
        self.clock = Clock()
        self.patch(WindowManager, 'count_waiting', lambda _, window_id: 100)

        self.wm = WindowManager(redis, window_size=10, flight_lifetime=10)
        self.add_cleanup(self.wm.stop)
        yield self.wm.create_window(self.window_id)
        self.redis = self.wm.redis

    @inlineCallbacks
    def test_race_condition(self):
        """
        A race condition can occur when multiple window managers try and
        access the same window at the same time.

        A LoopingCall loops over the available windows, for those windows
        it tries to get a next key. It does that by checking how many are
        waiting to be sent out and adding however many it can still carry
        to its own flight.

        Since there are concurrent workers, between the time of checking how
        many are available and how much room it has available, a different
        window manager may have already beaten it to it.

        If this happens Redis' `rpoplpush` method will return None since
        there are no more available keys for the given window.
        """
        yield self.wm.add(self.window_id, 1)
        yield self.wm.add(self.window_id, 2)
        yield self.wm._monitor_windows(lambda *a: True, True)
        self.assertEqual((yield self.wm.get_next_key(self.window_id)), None)
