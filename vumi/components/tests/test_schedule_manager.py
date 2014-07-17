"""Tests for go.apps.sequential_send.vumi_app"""

from datetime import datetime

from vumi.components.schedule_manager import ScheduleManager
from vumi.tests.utils import LogCatcher
from vumi.tests.helpers import VumiTestCase


class TestScheduleManager(VumiTestCase):
    def assert_schedule_next(self, config, since_dt, expected_next_dt):
        sm = ScheduleManager(config)
        self.assertEqual(sm.get_next(since_dt), expected_next_dt)

    def assert_config_error(self, config, errmsg):
        sm = ScheduleManager(config)
        with LogCatcher() as logger:
            self.assertEqual(None, sm.get_next(None))
            [err] = logger.errors
            self.assertEqual(err['why'], 'Error processing schedule.')
            self.assertEqual(err['failure'].value.args[0], errmsg)
        [f] = self.flushLoggedErrors(ValueError)
        self.assertEqual(f, err['failure'])

    def test_invalid_recurring(self):
        self.assert_config_error(
            {'recurring': 'No, iterate.'},
            "Invalid value for 'recurring': 'No, iterate.'")

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

    def test_daily_invalid_time(self):
        self.assert_config_error(
            {'recurring': 'daily', 'time': 'lunch time'},
            "time data 'lunch time' does not match format '%H:%M:%S'")

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

    def test_day_of_month_invalid_time(self):
        self.assert_config_error(
            {'recurring': 'day_of_month', 'time': 'lunch time'},
            "time data 'lunch time' does not match format '%H:%M:%S'")

    def test_day_of_month_invalid_days(self):
        self.assert_config_error(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': 'x'},
            "Invalid value for 'days': 'x'")
        self.assert_config_error(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '0 1'},
            "Invalid value for 'days': '0 1'")
        self.assert_config_error(
            {'recurring': 'day_of_month', 'time': '12:00:00', 'days': '32'},
            "Invalid value for 'days': '32'")

    def test_day_of_week_same_day(self):
        self.assert_schedule_next(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '2 4'},
            datetime(2012, 11, 20, 11, 0, 0),
            datetime(2012, 11, 20, 12, 0, 0))
        self.assert_schedule_next(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '2 4'},
            datetime(2012, 11, 20, 11, 0, 0),
            datetime(2012, 11, 20, 12, 0, 0))

    def test_day_of_week_same_week(self):
        self.assert_schedule_next(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '2 4'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 11, 22, 12, 0, 0))
        self.assert_schedule_next(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '6 7'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 11, 24, 12, 0, 0))

    def test_day_of_week_next_week(self):
        self.assert_schedule_next(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '1'},
            datetime(2012, 11, 20, 13, 0, 0),
            datetime(2012, 11, 26, 12, 0, 0))
        self.assert_schedule_next(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '1'},
            datetime(2012, 12, 20, 13, 0, 0),
            datetime(2012, 12, 24, 12, 0, 0))

    def test_day_of_week_invalid_time(self):
        self.assert_config_error(
            {'recurring': 'day_of_week', 'time': 'lunch time'},
            "time data 'lunch time' does not match format '%H:%M:%S'")

    def test_day_of_week_invalid_days(self):
        self.assert_config_error(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': 'x'},
            "Invalid value for 'days': 'x'")
        self.assert_config_error(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '0 1'},
            "Invalid value for 'days': '0 1'")
        self.assert_config_error(
            {'recurring': 'day_of_week', 'time': '12:00:00', 'days': '8'},
            "Invalid value for 'days': '8'")

    def test_never(self):
        self.assert_schedule_next(
            {'recurring': 'never'},
            datetime(2012, 11, 20, 13, 0, 0),
            None)
