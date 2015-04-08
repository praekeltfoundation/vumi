# -*- encoding: utf-8 -*-

"""Tests for vumi.blinkenlights.heartbeat.monitor"""

import json

from twisted.internet.defer import inlineCallbacks

from vumi.persist.txredis_manager import TxRedisManager
from vumi.blinkenlights.heartbeat import storage
from vumi.blinkenlights.heartbeat import monitor
from vumi.tests.helpers import VumiTestCase


class DummySystem:
    def __init__(self):
        self.system_id = 'haha'

    def dumps(self):
        return "Ha!"


class TestStorage(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
            'key_prefix': 'heartbeats',
            'db': 5,
            'FAKE_REDIS': True,
        }
        self.redis = yield TxRedisManager.from_config(config)
        self.add_cleanup(self.cleanup_redis)
        self.stg = storage.Storage(self.redis)

    @inlineCallbacks
    def cleanup_redis(self):
        yield self.redis._purge_all()
        yield self.redis.close_manager()

    @inlineCallbacks
    def test_add_system_ids(self):
        yield self.stg.add_system_ids(['foo', 'bar'])
        yield self.stg.add_system_ids(['bar'])
        res = yield self.redis.smembers(storage.SYSTEMS_KEY)
        self.assertEqual(sorted(res), sorted(['foo', 'bar']))

    @inlineCallbacks
    def test_write_system(self):
        yield self.stg.write_system(DummySystem())
        res = yield self.redis.get(storage.system_key('haha'))
        self.assertEqual(res, 'Ha!')

    @inlineCallbacks
    def test_delete_issue(self):
        iss = monitor.WorkerIssue('min-procs-fail', 5, 78)
        yield self.stg.open_or_update_issue('worker-1', iss)

        res = yield self.redis.get(storage.issue_key('worker-1'))
        self.assertEqual(type(res), str)

        yield self.stg.delete_worker_issue('worker-1')
        res = yield self.redis.get(storage.issue_key('worker-1'))
        self.assertEqual(res, None)

    @inlineCallbacks
    def test_open_or_update_issue(self):
        obj = {
            'issue_type': 'min-procs-fail',
            'start_time': 5,
            'procs_count': 78,
        }
        iss = monitor.WorkerIssue('min-procs-fail', 5, 78)
        yield self.stg.open_or_update_issue('foo', iss)
        res = yield self.redis.get(storage.issue_key('foo'))
        self.assertEqual(res, json.dumps(obj))

        # now update the issue
        iss = monitor.WorkerIssue('min-procs-fail', 5, 77)
        obj['procs_count'] = 77
        yield self.stg.open_or_update_issue('foo', iss)
        res = yield self.redis.get(storage.issue_key('foo'))
        self.assertEqual(res, json.dumps(obj))
