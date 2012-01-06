import time
from datetime import datetime

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.tests.utils import FakeRedis
from vumi.transports.scheduler import Scheduler
from vumi.message import TransportUserMessage


class SchedulerTestCase(TestCase):

    def setUp(self):
        self.r_server = FakeRedis()
        self.scheduler = Scheduler(self.r_server, self._scheduler_callback)
        self._delivery_history = []

    def tearDown(self):
        if self.scheduler.is_running:
            self.scheduler.stop()
        self._delivery_history = []

    def _scheduler_callback(self, scheduled_at, message):
        self._delivery_history.append((scheduled_at, message))
        return (scheduled_at, message)

    def assertDelivered(self, message):
        self.assertIn(message['message_id'],
            [message['message_id'] for _, message in self._delivery_history])

    def assertNumDelivered(self, number):
        self.assertEqual(number, len(self._delivery_history))

    def get_pending_messages(self):
        scheduled_timestamps = self.scheduler.r_key('scheduled_timestamps')
        return self.r_server.zrange(scheduled_timestamps, 0, -1)

    def mkmsg_in(self, content='hello world', message_id='abc',
                 to_addr='9292', from_addr='+41791234567',
                 session_event=None, transport_type='sms',
                 transport_name='test_transport',
                 helper_metadata={}, transport_metadata={}):
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr=to_addr,
            message_id=message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            helper_metadata=helper_metadata,
            content=content,
            session_event=session_event,
            timestamp=datetime.now(),
            )

    def test_scheduling(self):
        msg = self.mkmsg_in()
        now = time.mktime(datetime(2012, 1, 1).timetuple())
        delta = 10  # seconds from now
        key, bucket_key = self.scheduler.schedule_for_delivery(msg, delta, now)
        self.assertEqual(bucket_key, '%s#%s.%s' % (
            self.scheduler.r_prefix,
            'scheduled_keys',
            self.scheduler.get_next_write_timestamp(delta, now)
        ))
        scheduled_key = self.scheduler.get_scheduled_key(now)
        self.assertEqual(scheduled_key, None)
        scheduled_time = now + delta + self.scheduler.GRANULARITY
        scheduled_key = self.scheduler.get_scheduled_key(scheduled_time)
        self.assertTrue(scheduled_key)

    @inlineCallbacks
    def test_delivery_loop(self):
        msg = self.mkmsg_in()
        now = time.mktime(datetime(2012, 1, 1).timetuple())
        delta = 10  # seconds from now
        self.scheduler.schedule_for_delivery(msg, delta, now)
        scheduled_time = now + delta + self.scheduler.GRANULARITY
        yield self.scheduler.deliver_scheduled(scheduled_time)
        self.assertDelivered(msg)

    @inlineCallbacks
    def test_deliver_loop_future(self):
        now = time.mktime(datetime(2012, 1, 1).timetuple())
        for i in range(0, 3):
            msg = self.mkmsg_in(message_id='message_%s' % (i,))
            delta = i * 10
            self.scheduler.schedule_for_delivery(msg, delta, now)
            scheduled_time = now + delta + self.scheduler.GRANULARITY
            yield self.scheduler.deliver_scheduled(scheduled_time)
            self.assertNumDelivered(i + 1)

    @inlineCallbacks
    def test_deliver_ancient_messages(self):
        # something stuck in the queue since 1912 or scheduler hasn't
        # been running since 1912
        msg = self.mkmsg_in()
        way_back = time.mktime(datetime(1912, 1, 1).timetuple())
        scheduled_key = self.scheduler.schedule_for_delivery(msg, 0, way_back)
        self.assertTrue(scheduled_key)
        now = time.mktime(datetime.now().timetuple())
        yield self.scheduler.deliver_scheduled(now)
        self.assertDelivered(msg)
        self.assertEqual(self.get_pending_messages(), [])

    @inlineCallbacks
    def test_clear_scheduled_messages_for_msisdn(self):
        msg = self.mkmsg_in()
        now = time.mktime(datetime.now().timetuple())
        scheduled_time = now + self.scheduler.GRANULARITY
        key, bucket = self.scheduler.schedule_for_delivery(msg, 0,
                                                    scheduled_time)
        self.assertEqual(len(self.get_pending_messages()), 1)
        self.scheduler.clear_scheduled(key)
        yield self.scheduler.deliver_scheduled()
        self.assertEqual(self.r_server.hgetall(key), {})
        self.assertEqual(self.r_server.smembers(bucket), set())
        self.assertNumDelivered(0)
