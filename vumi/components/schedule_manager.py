# -*- test-case-name: vumi.components.tests.test_schedule_manager -*-

from datetime import datetime, timedelta

from vumi import log


class ScheduleManager(object):
    """Utility for determining whether a scheduled event is due.

    :class:`ScheduleManager` basically answers the question "are we there yet?"
    given a schedule definition, the last time the question was asked and the
    current time. It is designed to be used as part of a larger system that
    periodically checks for scheduled events.

    The schedule definition is a `dict` containing a mandatory `recurring`
    field which specifies the type of recurring schedule and other fields
    depending on the value of the `recurring` field.

    Currently, the following are supported:

     * `daily`
       The `time` field is required and specifies the (approximate) time of day
       the event is scheduled for in "HH:MM:SS" format.

     * `day_of_month`
       The `time` field is required and specifies the (approximate) time of day
       the event is scheduled for in "HH:MM:SS" format.
       The `days` field is required and specifies the days of the month the
       event is scheduled for as a list of comma/whitespace-separated integers.
    """

    def __init__(self, schedule_definition):
        self.schedule_definition = schedule_definition

    def is_scheduled(self, then, now):
        now_dt = datetime.utcfromtimestamp(now)
        then_dt = datetime.utcfromtimestamp(then)

        next_dt = self.get_next(then_dt)

        if next_dt is None:
            # We have an invalid schedule definition.
            return False

        return (next_dt <= now_dt)

    def get_next(self, since_dt):
        recurring_type = self.schedule_definition['recurring']
        if recurring_type == 'daily':
            return self.get_next_daily(since_dt)
        elif recurring_type == 'day_of_month':
            return self.get_next_day_of_month(since_dt)
        elif recurring_type == 'day_of_week':
            return self.get_next_day_of_week(since_dt)
        else:
            log.warning(
                "Invalid value for 'recurring': %r" % (recurring_type,))

    def get_next_daily(self, since_dt):
        timeofday = datetime.strptime(
            self.schedule_definition['time'], '%H:%M:%S').time()

        next_dt = datetime.combine(since_dt.date(), timeofday)
        while next_dt <= since_dt:
            next_dt += timedelta(days=1)

        return next_dt

    def get_next_day_of_month(self, since_dt):
        timeofday = datetime.strptime(
            self.schedule_definition['time'], '%H:%M:%S').time()
        days_str = self.schedule_definition['days'].replace(',', ' ')
        days_of_month = set([int(day) for day in days_str.split()])

        next_dt = datetime.combine(since_dt.date(), timeofday)
        while (next_dt.day not in days_of_month) or (next_dt <= since_dt):
            next_dt += timedelta(days=1)

        return next_dt

    def get_next_day_of_week(self, since_dt):
        timeofday = datetime.strptime(
            self.schedule_definition['time'], '%H:%M:%S').time()
        days_str = self.schedule_definition['days'].replace(',', ' ')
        days_of_week = set([int(day) for day in days_str.split()])

        next_dt = datetime.combine(since_dt.date(), timeofday)
        while ((next_dt.isoweekday() not in days_of_week)
               or (next_dt <= since_dt)):
            next_dt += timedelta(days=1)

        return next_dt
