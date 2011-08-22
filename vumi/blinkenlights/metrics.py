# -*- test-case-name: vumi.blinkenlights.tests.test_metrics -*-

from twisted.internet.task import LoopingCall
from vumi.service import Publisher
from vumi.blinkenlights.message20110818 import MetricMessage

import time


class MetricManager(Publisher):
    """Utility for creating and monitoring a set of metrics.

    Parameters
    ----------
    prefix : str
        Prefix for the name of all metrics created by this metric set.
    publish_interval : int in seconds
        How often to publish the set of metrics. This should match
        the finest retention level specified in Graphite if you
        are using Graphite to store the metric data points.
    """
    exchange_name = "vumi.metrics"
    exchange_type = "direct"
    routing_key = "vumi.metrics"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def __init__(self, prefix, publish_interval=5):
        self.prefix = prefix
        self._metrics = []  # list of metric objects
        self._publish_interval = publish_interval
        self._task = None  # created in .start()

    def start(self, channel):
        """Start publishing metrics in a loop."""
        super(MetricManager, self).start(channel)
        self._task = LoopingCall(self._publish_metrics)
        self._task.start(self._publish_interval)
        # TODO: capture deferred and add an errback to log errors?

    def stop(self):
        """Stop publishing metrics."""
        self._task.stop()
        self._task = None

    def _publish_metrics(self):
        msg = MetricMessage()
        now = time.time()
        for metric in self._metrics:
            msg.append((metric.name, now, metric.poll()))
        self.publish_message(msg)

    def register(self, metric):
        """Register a new metric object to be managed by this metric set.

        A metric can be registered with only on metric set.

        Parameters
        ----------
        metric : instance of :class:`Metric`
            Metric object to register. Will have prefix added to name.

        Returns
        -------
        metric : instance of :class:`Metric`
            For convenience, returns the metric passed in.
        """
        metric.manage(self.prefix)
        self._metrics.append(metric)
        return metric


class MetricRegistrationError(Exception):
    pass


class Metric(object):
    """Base clase for metrics.

    Parameters
    ----------
    suffix : str
        Partial name for the metric. The metric's full name
        will be constructed when it is registered with a
        :class:`MetricManager`.
    """
    def __init__(self, suffix):
        self.name = None  # set when prefix is set
        self._suffix = suffix

    def manage(self, prefix):
        """Called by a :class:`MetricSet` when this metric is registered."""
        if self.name is not None:
            raise MetricRegistrationError("Metric %s%s already registered"
                                          " with a MetricSet." %
                                          (prefix, self._suffix))
        self.name = prefix + self._suffix

    def poll(self):
        """Called periodically by the managing metric set."""
        raise NotImplementedError("Metric subclasses should implement .pol()")


class SimpleValue(Metric):
    """A metric representing a single value.

    If this metric's value is set repeatedly before it is polled all
    but the last value set will have no effect. Thus it only makes
    sense to use it for value that can sensibly be sampled at sparse
    time intervals (or for values that change very slowly).

    Examples
    --------
    >>> mm = MetricManager('vumi.worker0.')
    >>> my_val = mm.register(Simple('my.vale'))
    >>> my_val.set(???)
    """

    def __init__(self, *args, **kws):
        super(SimpleValue, self).__init__(*args, **kws)
        self._value = 0.0

    def set(self, value):
        """Set the current value."""
        self._value = value

    def poll(self):
        """Return the current value."""
        return self._value


class Count(Metric):
    """A simple counter.

    Examples
    --------
    >>> mm = MetricManager('vumi.worker0.')
    >>> my_count = mm.register(Count('my.count'))
    >>> my_count.inc()
    """

    def __init__(self, *args, **kws):
        super(Count, self).__init__(*args, **kws)
        self._value = 0

    def inc(self):
        """Increment the count by 1."""
        self._value += 1

    def poll(self):
        """Return the count and reset it to zero."""
        value, self._value = self._value, 0
        return value


class Sum(Metric):
    """A metric representing a sum of values.

    Examples
    --------
    >>> mm = MetricManager('vumi.worker0.')
    >>> my_sum = mm.register(Sum('my.sum'))
    >>> my_sum.add(1.0)
    >>> my_sum.add(0.5)
    """

    def __init__(self, *args, **kws):
        super(Sum, self).__init__(*args, **kws)
        self._value = 0.0

    def add(self, amount):
        """Add an amount to the sum."""
        self._value += amount

    def poll(self):
        """Return the sum and reset it to zero."""
        value, self._value = self._value, 0.0
        return value


class TimerAlreadyStartedError(Exception):
    pass


class Timer(Sum):
    """A metric that accumulates time spent on operations.

    Examples
    --------
    >>> mm = MetricManager('vumi.worker0.')
    >>> my_timer = mm.register(Timer('hard.work'))
    >>> with my_timer:
    >>>     process_data()
    >>> my_timer.start()
    >>> try:
    >>>     process_other_data()
    >>> finally:
    >>>     my_timer.stop()
    """
    def __init__(self, *args, **kws):
        super(Timer, self).__init__(*args, **kws)
        self._start_time = None

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    def start(self):
        if self._start_time is not None:
            raise TimerAlreadyStartedError("Attempt to start timer %s that "
                                           "was already started" %
                                           (self.name,))
        self._start_time = time.time()

    def stop(self):
        duration = time.time() - self._start_time
        self._start_time = None
        self.add(duration)
