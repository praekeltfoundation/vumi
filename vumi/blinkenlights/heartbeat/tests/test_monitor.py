# -*- encoding: utf-8 -*-

"""Tests for vumi.blinkenlights.heartbeat.monitor"""

import os
import time

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import get_stubbed_worker
from vumi.blinkenlights.heartbeat import publisher
from vumi.blinkenlights.heartbeat import monitor


class MockHeartBeatMonitor(monitor.HeartBeatMonitor):

    # stub out the LoopingCall task
    def _start_looping_task(self):
        self._task = None


class TestHeartBeatMonitor(TestCase):

    def setUp(self):
        self.worker = get_stubbed_worker(MockHeartBeatMonitor, config={})

    def tearDown(self):
        self.worker.stopWorker()

    def gen_fake_attrs(self, timestamp):
        attrs = {
            'version': publisher.HeartBeatMessage.VERSION_20130319,
            'system_id': "system-1",
            'worker_id': "worker-1",
            'hostname': "test-host-1",
            'timestamp': timestamp,
            'pid': os.getpid(),
        }
        return attrs

    @inlineCallbacks
    def test_update(self):
        """
        Test the processing of a message
        """

        yield self.worker.startWorker()

        attrs = self.gen_fake_attrs(time.time())

        # process the fake message
        self.worker.update(attrs)

        # retrieve the record corresponding to the worker in the fake message
        wkr_record = self.worker._ensure(attrs['system_id'],
                                         attrs['worker_id'])

        # and sanity test...
        self.assertEqual(wkr_record['system_id'], attrs['system_id'])
        self.assertEqual(wkr_record['worker_id'], attrs['worker_id'])
        self.assertEqual(wkr_record['events'][-1].state, monitor.Event.ALIVE)

    @inlineCallbacks
    def test_find_missing_workers(self):
        """ Test that we correctly identify missing workers """

        yield self.worker.startWorker()

        deadline = 100

        # create a fake message which is on-time
        msg = self.gen_fake_attrs(deadline)
        self.worker.update(msg)

        # verify that the worker is not late
        missing = self.worker._find_missing_workers(deadline)
        self.assertEqual(missing, [])

        # create a fake message which is late
        msg = self.gen_fake_attrs(deadline - 1)
        self.worker.update(msg)

        # verify that the worker is found to be late
        missing = self.worker._find_missing_workers(deadline)
        self.assertEqual(len(missing), 1)
        self.assertEqual(missing[0]['worker_id'], msg['worker_id'])
