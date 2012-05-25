"""Tests for vumi.application.session."""

import time

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from vumi.tests.utils import FakeRedis
from vumi.application import SessionManager, TxSessionManager


class SessionManagerTestCase(TestCase):
    def setUp(self):
        self.fake_redis = FakeRedis()
        self.sm = SessionManager(self.fake_redis, prefix="test")

    def tearDown(self):
        self.sm.stop()
        self.fake_redis.teardown()

    def test_active_sessions(self):
        def get_sessions():
            return sorted(self.sm.active_sessions())

        def ids():
            return [x[0] for x in get_sessions()]

        self.assertEqual(ids(), [])
        self.sm.create_session("u1")
        self.assertEqual(ids(), ["u1"])
         # 10 seconds later
        self.sm.create_session("u2", created_at=time.time() + 10)
        self.assertEqual(ids(), ["u1", "u2"])

        s1, s2 = get_sessions()
        self.assertTrue(s1[1]['created_at'] < s2[1]['created_at'])

    def test_schedule_session_expiry(self):
        self.sm.max_session_length = 60.0
        self.sm.create_session("u1")

    def test_create_and_retrieve_session(self):
        session = self.sm.create_session("u1")
        self.assertEqual(sorted(session.keys()), ['created_at'])
        self.assertTrue(time.time() - float(session['created_at']) < 10.0)
        loaded = self.sm.load_session("u1")
        self.assertEqual(loaded, session)

    def test_save_session(self):
        test_session = {"foo": 5, "bar": "baz"}
        self.sm.create_session("u1")
        self.sm.save_session("u1", test_session)
        session = self.sm.load_session("u1")
        self.assertTrue(session.pop('created_at') is not None)
        # Redis saves & returns all session values as strings
        self.assertEqual(session, dict([map(str, kvs) for kvs
                                        in test_session.items()]))

    def test_lazy_clearing(self):
        self.sm.save_session('user_id', {})
        self.assertEqual(list(self.sm.active_sessions()), [])


class TxSessionManagerTestCase(TestCase):
    @inlineCallbacks
    def setUp(self):
        from vumi.utils import txredis_from_config
        self.txredis = yield txredis_from_config({})
        yield self.clear_keys()
        self.sm = TxSessionManager(self.txredis, prefix="test")

    @inlineCallbacks
    def clear_keys(self):
        for key in (yield self.txredis.keys('test:*')):
            yield self.txredis.delete(key)

    @inlineCallbacks
    def tearDown(self):
        self.sm.stop()
        yield self.clear_keys()
        yield self.txredis.factory.stopTrying()
        yield self.txredis.transport.loseConnection()

    @inlineCallbacks
    def test_active_sessions(self):
        def get_sessions():
            return self.sm.active_sessions().addCallback(lambda s: sorted(s))

        def ids():
            return get_sessions().addCallback(lambda s: [x[0] for x in s])

        self.assertEqual((yield ids()), [])
        yield self.sm.create_session("u1")
        self.assertEqual((yield ids()), ["u1"])
         # 10 seconds later
        yield self.sm.create_session("u2", created_at=time.time() + 10)
        self.assertEqual((yield ids()), ["u1", "u2"])

        s1, s2 = yield get_sessions()
        self.assertTrue(s1[1]['created_at'] < s2[1]['created_at'])

    @inlineCallbacks
    def test_schedule_session_expiry(self):
        self.sm.max_session_length = 60.0
        yield self.sm.create_session("u1")

    @inlineCallbacks
    def test_create_and_retrieve_session(self):
        session = yield self.sm.create_session("u1")
        self.assertEqual(sorted(session.keys()), ['created_at'])
        self.assertTrue(time.time() - float(session['created_at']) < 10.0)
        loaded = yield self.sm.load_session("u1")
        self.assertEqual(loaded, session)

    @inlineCallbacks
    def test_save_session(self):
        test_session = {"foo": 5, "bar": "baz"}
        yield self.sm.create_session("u1")
        yield self.sm.save_session("u1", test_session)
        session = yield self.sm.load_session("u1")
        self.assertTrue(session.pop('created_at') is not None)
        # Redis saves & returns all session values as strings
        self.assertEqual(session, dict([map(str, kvs) for kvs
                                        in test_session.items()]))

    @inlineCallbacks
    def test_lazy_clearing(self):
        yield self.sm.save_session('user_id', {})
        self.assertEqual(list((yield self.sm.active_sessions())), [])
