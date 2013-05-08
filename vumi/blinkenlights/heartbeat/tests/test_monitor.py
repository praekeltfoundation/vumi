# -*- encoding: utf-8 -*-

"""Tests for vumi.blinkenlights.heartbeat.monitor"""

import os
import time
import json

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import get_stubbed_worker
from vumi.blinkenlights.heartbeat import publisher
from vumi.blinkenlights.heartbeat import monitor
from vumi.blinkenlights.heartbeat.storage import (hostinfo_key,
                                                  attr_key)
from vumi.utils import generate_worker_id


class MockHeartBeatMonitor(monitor.HeartBeatMonitor):

    # stub out the LoopingCall task
    def _start_looping_task(self):
        self._task = None


class TestHeartBeatMonitor(TestCase):

    def setUp(self):
        config = {
            'deadline': 30,
            'redis_manager': {
                'key_prefix': 'heartbeats',
                'db': 5,
                'FAKE_REDIS': True,
            },
            'monitored_systems': {
                'system-1': {
                    'system_name': 'system-1',
                    'system_id': 'system-1',
                    'workers': {
                        'twitter_transport': {
                            'name': 'twitter_transport',
                            'min_procs': 2,
                        }
                    }
                }
            }
        }
        self.worker = get_stubbed_worker(monitor.HeartBeatMonitor, config)

    def tearDown(self):
        self.worker.stopWorker()

    def gen_fake_attrs(self, timestamp):
        sys_id = 'system-1'
        wkr_name = 'twitter_transport'
        wkr_id = generate_worker_id(sys_id, wkr_name)
        attrs = {
            'version': publisher.HeartBeatMessage.VERSION_20130319,
            'system_id': sys_id,
            'worker_id': wkr_id,
            'worker_name': wkr_name,
            'hostname': "test-host-1",
            'timestamp': timestamp,
            'pid': os.getpid(),
        }
        return attrs

    @inlineCallbacks
    def test_update(self):
        """
        Test the processing of a message.

        """

        yield self.worker.startWorker()

        attrs1 = self.gen_fake_attrs(time.time())
        attrs2 = self.gen_fake_attrs(time.time())

        # process the fake message (and process it twice to verify idempotency)
        self.worker.update(attrs1)
        self.worker.update(attrs1)

        # retrieve the instance set corresponding to the worker_id in the
        # fake message
        instance_set = self.worker._instance_sets[attrs1['worker_id']]
        self.assertEqual(len(instance_set), 1)
        inst = instance_set.pop()
        instance_set.add(inst)
        self.assertEqual(inst.hostname, "test-host-1")
        self.assertEqual(inst.pid, os.getpid())

        # now process a message from another instance of the worker
        # and verify that there are two recorded instances
        attrs2['hostname'] = 'test-host-2'
        self.worker.update(attrs2)
        self.assertEqual(len(instance_set), 2)

    @inlineCallbacks
    def test_verify_workers_fail(self):
        # here we test the verification of a worker who
        # who had less than min_procs check in

        yield self.worker.startWorker()
        fkredis = self.worker._redis

        attrs = self.gen_fake_attrs(time.time())
        wkr_id = attrs['worker_id']
        # process the fake message ()
        self.worker.update(attrs)

        lst_fail, lst_pass = self.worker._verify_workers()
        # test return values
        self.assertEqual(len(lst_fail), 1)
        self.assertEqual(len(lst_pass), 0)
        self.assertEqual(lst_fail[0][0], attrs['worker_id'])  # worker id
        self.assertEqual(lst_fail[0][1], 1)  # proc count

        # test that hostinfo was persisted into redis
        key = hostinfo_key(wkr_id)
        hostinfo = json.loads((yield fkredis.get(key)))
        self.assertEqual(hostinfo['test-host-1'], 1)

    @inlineCallbacks
    def test_verify_workers_pass(self):
        # here we test the verification of a worker who
        # checked in more than min_procs

        yield self.worker.startWorker()
        fkredis = self.worker._redis

        attrs = self.gen_fake_attrs(time.time())
        wkr_id = attrs['worker_id']
        # process the fake message ()
        self.worker.update(attrs)
        attrs['hostname'] = 'test-host-2'
        self.worker.update(attrs)
        attrs['pid'] = 23
        # and for kicks, change pid, so that 2 instances are registered
        # as having checked in for test-host-2
        self.worker.update(attrs)

        lst_fail, lst_pass = self.worker._verify_workers()
        # test return values
        self.assertEqual(len(lst_fail), 0)
        self.assertEqual(len(lst_pass), 1)
        self.assertEqual(lst_pass[0][0], attrs['worker_id'])  # worker id

        # test that hostinfo was persisted into redis
        key = hostinfo_key(wkr_id)
        hostinfo = json.loads((yield fkredis.get(key)))
        self.assertEqual(hostinfo['test-host-1'], 1)
        self.assertEqual(hostinfo['test-host-2'], 2)

    @inlineCallbacks
    def test_sync_to_redis(self):
        """
        This covers a lot of the storage API as well the
        Monitor's _sync_to_redis() function.
        """
        yield self.worker.startWorker()
        fkredis = self.worker._redis

        # Systems
        systems = json.loads((yield fkredis.get('systems')))
        self.assertEqual(tuple(systems), ('system-1',))

        # Workers in System
        workers = json.loads((yield fkredis.get("system:system-1:workers")))
        self.assertEqual(tuple(workers), ('system-1:twitter_transport',))

        # Worker attrs
        wkr_id = generate_worker_id('system-1', 'twitter_transport')
        attrs = json.loads((yield fkredis.get(attr_key(wkr_id))))
        self.assertEqual(attrs['worker_name'], 'twitter_transport')
        self.assertEqual(attrs['min_procs'], 2)
