# -*- test-case-name: vumi.blinkenlights.tests.test_metrics -*-

"""Basic set of functionality for working with blinkenlights metrics.

Includes a publisher, a consumer and a set of simple metrics.
"""

import time
import warnings

from twisted.internet.task import LoopingCall
from twisted.python import log
from zope.interface import Interface, implementer

from vumi.service import Publisher, Consumer
from vumi.blinkenlights.message20110818 import MetricMessage


class IMetricPublisher(Interface):
    def publish_message(msg):
        """
        Publish a :class:`MetricMessage`.
        """


@implementer(IMetricPublisher)
class MetricPublisher(Publisher):
    """
    Publisher for metrics messages.
    """
    exchange_name = "vumi.metrics"
    exchange_type = "direct"
    routing_key = "vumi.metrics"
    durable = True
    auto_delete = False
    delivery_mode = 2


class MetricManager(object):
    """Utility for creating and monitoring a set of metrics.

    :type prefix: str
    :param prefix:
        Prefix for the name of all metrics registered with this manager.
    :type publish_interval: int in seconds
    :param publish_interval:
        How often to publish the set of metrics.
    :type on_publish: f(metric_manager)
    :param on_publish:
        Function to call immediately after metrics after published.
    """

    def __init__(self, prefix, publish_interval=5, on_publish=None,
                 publisher=None):
        self.prefix = prefix
        self._metrics = []  # list of metrics to poll
        self._oneshot_msgs = []  # list of oneshot messages since last publish
        self._metrics_lookup = {}  # metric name -> metric
        self._publish_interval = publish_interval
        self._task = None  # created in .start()
        self._on_publish = on_publish
        self._publisher = publisher

    def start_polling(self):
        """
        Start the metric polling and publishing task.
        """
        self._task = LoopingCall(self.publish_metrics)
        done = self._task.start(self._publish_interval, now=False)
        done.addErrback(lambda failure: log.err(failure,
                        "MetricManager polling task died"))

    def stop_polling(self):
        """
        Stop the metric polling and publishing task.
        """
        if self._task:
            self._task.stop()
            self._task = None

    def publish_metrics(self):
        """
        Publish all waiting metrics.
        """
        msg = MetricMessage()
        self._collect_oneshot_metrics(msg)
        self._collect_polled_metrics(msg)
        self.publish_message(msg)
        if self._on_publish is not None:
            self._on_publish(self)

    def publish_message(self, msg):
        if self._publisher is None:
            raise ValueError("No publisher available.")
        IMetricPublisher(self._publisher).publish_message(msg)

    def _collect_oneshot_metrics(self, msg):
        oneshots, self._oneshot_msgs = self._oneshot_msgs, []
        for metric, values in oneshots:
            msg.append((self.prefix + metric.name, metric.aggs, values))

    def _collect_polled_metrics(self, msg):
        for metric in self._metrics:
            msg.append((self.prefix + metric.name, metric.aggs, metric.poll()))

    def oneshot(self, metric, value):
        """Publish a single value for the given metric.

        :type metric: :class:`Metric`
        :param metric:
            Metric object to register. Will have the manager's prefix
            added to its name.
        :type value: float
        :param value:
            The value to publish for the metric.
        """
        self._oneshot_msgs.append(
            (metric, [(int(time.time()), value)]))

    def register(self, metric):
        """Register a new metric object to be managed by this metric set.

        A metric can be registered with only one metric set.

        :type metric: :class:`Metric`
        :param metric:
            Metric object to register. The metric will have its `.manage()`
            method called with this manager as the manager.
        :rtype:
            For convenience, returns the metric passed in.
        """
        metric.manage(self)
        self._metrics.append(metric)
        if metric.name in self._metrics_lookup:
            raise MetricRegistrationError("Duplicate metric name %s"
                                          % metric.name)
        self._metrics_lookup[metric.name] = metric
        return metric

    def __getitem__(self, suffix):
        return self._metrics_lookup[suffix]

    def __contains__(self, suffix):
        return suffix in self._metrics_lookup

    # Everything from this point onward is to allow MetricManager to pretend to
    # be a publisher and avoid breaking existing code that treats it as one.

    exchange_name = MetricPublisher.exchange_name
    exchange_type = MetricPublisher.exchange_type
    durable = MetricPublisher.durable

    _publish_metrics = publish_metrics  # For old tests that poke this.

    def start(self, channel):
        """Start publishing metrics in a loop."""
        if self._publisher is not None:
            raise RuntimeError("Publisher already present.")
        self._publisher = MetricPublisher()
        self._publisher.start(channel)
        self.start_polling()

    def stop(self):
        """Stop publishing metrics."""
        self.stop_polling()


class AggregatorAlreadyDefinedError(Exception):
    pass


class Aggregator(object):
    """Registry of aggregate functions for metrics.

    :type name: str
    :param name:
       Short name for the aggregator.
    :type func: f(list of values) -> float
    :param func:
       The aggregation function. Should return a default value
       if the list of values is empty (usually this default is 0.0).
    """

    REGISTRY = {}

    def __init__(self, name, func):
        if name in self.REGISTRY:
            raise AggregatorAlreadyDefinedError(name)
        self.name = name
        self.func = func
        self.REGISTRY[name] = self

    @classmethod
    def from_name(cls, name):
        return cls.REGISTRY[name]

    def __call__(self, values):
        return self.func(values)


SUM = Aggregator("sum", sum)
AVG = Aggregator("avg",
                 lambda values: sum(values) / len(values) if values else 0.0)
MAX = Aggregator("max", lambda values: max(values) if values else 0.0)
MIN = Aggregator("min", lambda values: min(values) if values else 0.0)
LAST = Aggregator("last", lambda values: values[-1] if values else 0.0)


class MetricRegistrationError(Exception):
    pass


class Metric(object):
    """Simple metric.

    Values set are collected and polled periodically by the metric
    manager.

    :type name: str
    :param name:
        Name of this metric. Will be appened to the
        :class:`MetricManager` prefix when this metric
        is published.
    :type aggregators: list of aggregators, optional
    :param aggregators:
        List of aggregation functions to request
        eventually be applied to this metric. The
        default is to average the value.

    Examples:

    >>> mm = MetricManager('vumi.worker0.')
    >>> my_val = mm.register(Metric('my.value'))
    >>> my_val.set(1.5)
    >>> my_val.name
    'my.value'
    """

    #: Default aggregators are [:data:`AVG`]
    DEFAULT_AGGREGATORS = [AVG]

    def __init__(self, name, aggregators=None):
        if aggregators is None:
            aggregators = self.DEFAULT_AGGREGATORS
        self.name = name
        self.aggs = tuple(sorted(agg.name for agg in aggregators))
        self._manager = None
        self._values = []  # list of unpolled values

    @property
    def managed(self):
        return self._manager is not None

    def manage(self, manager):
        """Called by :class:`MetricManager` when this metric is registered."""
        if self._manager is not None:
            raise MetricRegistrationError(
                "Metric %s already registered with MetricManager with"
                " prefix %s." % (self.name, self._manager.prefix))
        self._manager = manager

    def set(self, value):
        """Append a value for later polling."""
        self._values.append((int(time.time()), value))

    def poll(self):
        """Called periodically by the :class:`MetricManager`."""
        values, self._values = self._values, []
        return values


class Count(Metric):
    """A simple counter.

    Examples:

    >>> mm = MetricManager('vumi.worker0.')
    >>> my_count = mm.register(Count('my.count'))
    >>> my_count.inc()
    """

    #: Default aggregators are [:data:`SUM`]
    DEFAULT_AGGREGATORS = [SUM]

    def inc(self):
        """Increment the count by 1."""
        self.set(1.0)


class TimerError(Exception):
    """Raised when an error occurs in a call to an EventTimer method."""


class TimerAlreadyStartedError(TimerError):
    """Raised when attempting to start an EventTimer that is already started.
    """


class TimerNotStartedError(TimerError):
    """Raised when attempting to stop an EventTimer that was not started.
    """


class TimerAlreadyStoppedError(TimerError):
    """Raised when attempting to stop an EventTimer that is already stopped.
    """


class EventTimer(object):
    def __init__(self, timer, start=False):
        self._timer = timer
        self._start_time = None
        self._stop_time = None
        if start:
            self.start()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False

    def start(self):
        if self._start_time is not None:
            raise TimerAlreadyStartedError("Attempt to start timer %r that"
                                           " was already started" %
                                           (self._timer.name,))
        self._start_time = time.time()

    def stop(self):
        if self._start_time is None:
            raise TimerNotStartedError("Attempt to stop timer %r that"
                                       " has not been started" %
                                       (self._timer.name,))
        if self._stop_time is not None:
            raise TimerAlreadyStoppedError("Attempt to stop timer %r that"
                                           " has already been stopped" %
                                           (self._timer.name,))
        self._stop_time = time.time()
        self._timer.set(self._stop_time - self._start_time)


class Timer(Metric):
    """A metric that records time spent on operations.

    Examples:

    >>> mm = MetricManager('vumi.worker0.')
    >>> my_timer = mm.register(Timer('hard.work'))

    Using the timer as a context manager:

    >>> with my_timer.timeit():
    >>>     process_data()

    Using the timer without a context manager:

    >>> event_timer = my_timer.timeit()
    >>> event_timer.start()
    >>> d = process_other_data()
    >>> d.addCallback(lambda r: event_timer.stop())

    Note that timers returned by `timeit` may only have `start` and `stop`
    called on them once (and only in that order).

    .. note::

       Using ``.start()`` or ``.stop()`` directly or via using the
       :class:`Timer` instance itself as a context manager is
       deprecated because they are not re-entrant and it's easy to
       accidentally overlap multiple calls to ``.start()`` and ``.stop()`` on
       the same :class:`Timer` instance (e.g. by letting the reactor run in
       between).

       All applications should be updated to use ``.timeit()``.

       Deprecated use of ``.start()`` and ``.stop()``:

       >>> my_timer.start()
       >>> try:
       >>>     process_other_data()
       >>> finally:
       >>>     my_timer.stop()

       Deprecated use of ``.start()`` and ``.stop()`` via using the
       :class:`Timer` itself as a context manager:

       >>> with my_timer:
       >>>     process_more_data()
    """

    #: Default aggregators are [:data:`AVG`]
    DEFAULT_AGGREGATORS = [AVG]

    def __init__(self, *args, **kws):
        super(Timer, self).__init__(*args, **kws)
        self._event_timer = EventTimer(self)

    def __enter__(self):
        warnings.warn(
            "Use of Timer directly as a context manager is deprecated."
            " Please use Timer.timeit() instead.",
            DeprecationWarning)
        return self._event_timer.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        result = self._event_timer.__exit__(exc_type, exc_val, exc_tb)
        self._event_timer = EventTimer(self)
        return result

    def timeit(self, start=False):
        return EventTimer(self, start=start)

    def start(self):
        warnings.warn(
            "Use of Timer.start() is deprecated."
            " Please use Timer.timeit() instead.",
            DeprecationWarning)
        return self._event_timer.start()

    def stop(self):
        result = self._event_timer.stop()
        self._event_timer = EventTimer(self)
        return result


class MetricsConsumer(Consumer):
    """Utility for consuming metrics published by :class:`MetricManager`s.

    :type callback: f(metric_name, aggregators, values)
    :param callback:
        Called for each metric datapoint as it arrives.
        The parameters are metric_name (str),
        aggregator (list of aggregator names) and values (a
        list of timestamp and value paits).
    """
    exchange_name = "vumi.metrics"
    exchange_type = "direct"
    routing_key = "vumi.metrics"
    durable = True

    def __init__(self, channel, callback):
        self.queue_name = self.routing_key
        super(MetricsConsumer, self).__init__(channel)
        self.callback = callback

    def consume_message(self, vumi_message):
        msg = MetricMessage.from_dict(vumi_message.payload)
        for metric_name, aggregators, values in msg.datapoints():
            self.callback(metric_name, aggregators, values)
