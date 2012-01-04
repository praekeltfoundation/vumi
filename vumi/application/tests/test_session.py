"""Tests for vumi.application.session."""

import time
from twisted.trial.unittest import TestCase
from vumi.tests.utils import FakeRedis
from vumi.application import SessionManager


class SessionManagerTestCase(TestCase):
    def setUp(self):
        self.sm = SessionManager(db=0, prefix="test")
        self.sm.r_server = FakeRedis()  # stub out redis

    def tearDown(self):
        self.sm.stop()
        self.sm.r_server.teardown()  # teardown fake redis

    def test_active_sessions(self):
        def get_sessions():
            return sorted(self.sm.active_sessions())

        def ids():
            return [x[0] for x in get_sessions()]

        self.assertEqual(ids(), [])
        self.sm.create_session("u1")
        self.assertEqual(ids(), ["u1"])
        self.sm.create_session("u2")
        self.assertEqual(ids(), ["u1", "u2"])

        s1, s2 = get_sessions()
        self.assertTrue(s1[1]['created_at'] < s2[1]['created_at'])

    def test_schedule_session_expiry(self):
        self.sm.max_session_length = 60.0
        self.sm.create_session("u1")

    def test_create_and_retreive_session(self):
        session = self.sm.create_session("u1")
        self.assertEqual(sorted(session.keys()), ['created_at'])
        self.assertTrue(time.time() - session['created_at'] < 10.0)
        loaded = self.sm.load_session("u1")
        self.assertEqual(loaded, session)

    def test_save_session(self):
        test_session = {"foo": 5, "bar": "baz"}
        self.sm.create_session("u1")
        self.sm.save_session("u1", test_session)
        session = self.sm.load_session("u1")
        self.assertTrue(session.pop('created_at') is not None)
        self.assertEqual(session, test_session)
