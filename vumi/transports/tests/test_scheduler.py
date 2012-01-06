import time
from pprint import pprint
from datetime import datetime, timedelta

from twisted.trial.unittest import TestCase
from twisted.python import log

from vumi.tests.utils import FakeRedis
from vumi.transports.scheduler import Scheduler
from vumi.message import TransportUserMessage


class SchedulerTestCase(TestCase):

    def setUp(self):
        self.r_server = FakeRedis()
        self.scheduler = Scheduler(self.r_server, log.msg)

    def tearDown(self):
        if self.scheduler.is_running:
            self.scheduler.stop()

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
        now = time.mktime(datetime(2012,1,1).timetuple())
        delta = 10  # seconds from now
        self.scheduler.schedule_for_delivery(msg, delta, now)
        scheduled_key = self.scheduler.get_scheduled_key(now)
        self.assertEqual(scheduled_key, None)
        scheduled_key = self.scheduler.get_scheduled_key(now + delta + self.scheduler.GRANULARITY)
        self.assertTrue(scheduled_key)
