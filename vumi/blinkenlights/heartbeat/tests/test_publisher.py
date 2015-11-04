# -*- encoding: utf-8 -*-

"""Tests for vumi.blinkenlights.heartbeat.publisher"""

import json

from twisted.internet.defer import inlineCallbacks

from vumi.blinkenlights.heartbeat import publisher
from vumi.errors import MissingMessageField
from vumi.tests.helpers import VumiTestCase, WorkerHelper


class MockHeartBeatPublisher(publisher.HeartBeatPublisher):

    # stub out the LoopingCall task
    def _start_looping_task(self):
        self._task = None


class TestHeartBeatPublisher(VumiTestCase):

    def gen_fake_attrs(self):
        attrs = {
            'version': publisher.HeartBeatMessage.VERSION_20130319,
            'system_id': "system-1",
            'worker_id': "worker-1",
            'worker_name': "worker-1",
            'hostname': "test-host-1",
            'timestamp': 100,
            'pid': 43,
        }
        return attrs

    @inlineCallbacks
    def test_publish_heartbeat(self):
        worker_helper = self.add_helper(WorkerHelper())
        client = worker_helper.get_fake_amqp_client(worker_helper.broker)
        client.startService()
        channel = yield client.await_connected().addCallback(
            lambda c: c.get_channel())
        pub = MockHeartBeatPublisher(self.gen_fake_attrs)
        pub.start(channel)
        pub._beat()

        yield worker_helper.kick_delivery()
        [msg] = worker_helper.broker.get_dispatched(
            "vumi.health", "heartbeat.inbound")
        self.assertEqual(json.loads(msg), self.gen_fake_attrs())

    def test_message_validation(self):
        attrs = self.gen_fake_attrs()
        attrs.pop("version")
        self.assertRaises(MissingMessageField, publisher.HeartBeatMessage,
                          **attrs)
        attrs = self.gen_fake_attrs()
        attrs.pop("system_id")
        self.assertRaises(MissingMessageField, publisher.HeartBeatMessage,
                          **attrs)
        attrs = self.gen_fake_attrs()
        attrs.pop("worker_id")
        self.assertRaises(MissingMessageField, publisher.HeartBeatMessage,
                          **attrs)
