"""Tests for go.apps.sequential_send.vumi_app"""

from datetime import datetime

from twisted.trial.unittest import TestCase

from vumi.components.schedule_manager import ScheduleManager


class ScheduleManagerTestCase(TestCase):
    def assert_schedule_next(self, config, since_dt, expected_next_dt):
        sm = ScheduleManager(config)
        self.assertEqual(sm.get_next(since_dt), expected_next_dt)

    def test_daily_schedule_same_day(self):
        self.assert_schedule_next(
            {'recurring': 'daily', 'time': '12:00:00'},
            datetime(2012, 11, 20, 11, 0, 0),
            datetime(2012, 11, 20, 12, 0, 0))

    def test_daily_schedule_next_day(self):
        self.assert_schedule_next(
            {'recurring': 'daily', 'time': '12:00:00'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 11, 21, 12, 0, 0))

    def test_day_of_month_same_day(self):
        self.assert_schedule_next(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '20 25'},
            datetime(2012, 11, 20, 11, 0, 0),
            datetime(2012, 11, 20, 12, 0, 0))
        self.assert_schedule_next(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '15 20'},
            datetime(2012, 11, 20, 11, 0, 0),
            datetime(2012, 11, 20, 12, 0, 0))

    def test_day_of_month_same_month(self):
        self.assert_schedule_next(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '20 25'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 11, 25, 12, 0, 0))
        self.assert_schedule_next(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '15 25'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 11, 25, 12, 0, 0))

    def test_day_of_month_next_month(self):
        self.assert_schedule_next(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '15 20'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 12, 15, 12, 0, 0))
        self.assert_schedule_next(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '1 15'},
            datetime(2012, 12, 20, 13, 0, 0),
            datetime(2013, 1, 1, 12, 0, 0))
