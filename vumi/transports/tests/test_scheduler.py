import time
from pprint import pprint
from datetime import datetime, timedelta

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.python import log

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
        self.scheduler.schedule_for_delivery(msg, delta, now)
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
        for i in range(0,3):
            msg = self.mkmsg_in(message_id='message_%s' % (i,))
            delta = i * 10
            self.scheduler.schedule_for_delivery(msg, delta, now)
            scheduled_time = now + delta + self.scheduler.GRANULARITY
            yield self.scheduler.deliver_scheduled(scheduled_time)
            self.assertNumDelivered(i + 1)