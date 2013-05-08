import os
import time
import json

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.tests.utils import (get_stubbed_worker,
                              get_stubbed_channel)
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.blinkenlights.heartbeat import publisher
from vumi.blinkenlights.heartbeat import monitor
from vumi.errors import MissingMessageField


class MockHeartBeatMonitor(monitor.HeartBeatMonitor):

    # stub out the LoopingCall task
    def _start_looping_task(self):
        self._task = None


class MockHeartBeatPublisher(publisher.HeartBeatPublisher):

    # stub out the LoopingCall task
    def _start_looping_task(self):
        self._task = None


class TestHeartBeatPublisher(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def gen_fake_attrs(self):
        attrs = {
            'version': publisher.HeartBeatMessage.VERSION_20130319,
            'system_id': "system-1",
            'worker_id': "worker-1",
            'hostname': "test-host-1",
            'timestamp': 100,
            'pid': 43,
            }
        return attrs

    @inlineCallbacks
    def test_publish_heartbeat(self):
        self.broker = FakeAMQPBroker()
        channel = yield get_stubbed_channel(self.broker)
        pub = MockHeartBeatPublisher(self.gen_fake_attrs)
        pub.start(channel)
        pub._beat()

        [msg] = self.broker.get_dispatched("vumi.health", "heartbeat.inbound")
        self.assertEqual(json.loads(msg.body), self.gen_fake_attrs())

    def test_message_validation(self):
        attrs = self.gen_fake_attrs()
        attrs.pop("version")
        self.assertRaises(MissingMessageField,
                          publisher.HeartBeatMessage, **attrs)
        attrs = self.gen_fake_attrs()
        attrs.pop("system_id")
        self.assertRaises(MissingMessageField,
                          publisher.HeartBeatMessage, **attrs)
        attrs = self.gen_fake_attrs()
        attrs.pop("worker_id")
        self.assertRaises(MissingMessageField,
                          publisher.HeartBeatMessage, **attrs)


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
        wkr_record = self.worker._ensure(
            attrs['system_id'], attrs['worker_id'])

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
