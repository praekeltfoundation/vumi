# -*- test-case-name: vumi.blinkenlights.tests.test_metrics -*-

"""Basic set of functionality for working with blinkenlights metrics.

Includes a publisher, a consumer and a set of simple metrics.
"""

from twisted.internet.task import LoopingCall
from twisted.python import log

from vumi.service import Publisher, Consumer
from vumi.blinkenlights.message20110818 import MetricMessage

import time


class MetricManager(Publisher):
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
    exchange_name = "vumi.metrics"
    exchange_type = "direct"
    routing_key = "vumi.metrics"
    durable = True
    auto_delete = False
    delivery_mode = 2

    def __init__(self, prefix, publish_interval=5, on_publish=None):
        self.prefix = prefix
        self._metrics = []  # list of metric objects
        self._metrics_lookup = {}  # metric suffix -> metric
        self._publish_interval = publish_interval
        self._task = None  # created in .start()
        self._on_publish = on_publish

    def start(self, channel):
        """Start publishing metrics in a loop."""
        super(MetricManager, self).start(channel)
        self._task = LoopingCall(self._publish_metrics)
        done = self._task.start(self._publish_interval, now=False)
        done.addErrback(lambda failure: log.err(failure,
                        "MetricManager publishing task died"))

    def stop(self):
        """Stop publishing metrics."""
        self._task.stop()
        self._task = None

    def _publish_metrics(self):
        msg = MetricMessage()
        for metric in self._metrics:
            msg.append((metric.name, metric.aggs, metric.poll()))
        self.publish_message(msg)
        if self._on_publish is not None:
            self._on_publish(self)

    def register(self, metric):
        """Register a new metric object to be managed by this metric set.

        A metric can be registered with only one metric set.

        :type metric: :class:`Metric`
        :param metric:
            Metric object to register. Will have the manager's prefix
            added to its name.
        :rtype:
            For convenience, returns the metric passed in.
        """
        metric.manage(self.prefix)
        self._metrics.append(metric)
        if metric.suffix in self._metrics_lookup:
            raise MetricRegistrationError("Duplicate metric name %s"
                                          % metric.name)
        self._metrics_lookup[metric.suffix] = metric
        return metric

    def __getitem__(self, suffix):
        return self._metrics_lookup[suffix]

    def __contains__(self, suffix):
        return suffix in self._metrics_lookup


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


class MetricRegistrationError(Exception):
    pass


class Metric(object):
    """Simple metric.

    Values set are collected and polled periodically by the metric
    manager.

    :type suffix: str
    :param suffix:
        Suffix to append to the :class:`MetricManager`
        prefix to create the metric name.
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
    'vumi.worker0.my.value'
    """

    #: Default aggregators are [:data:`AVG`]
    DEFAULT_AGGREGATORS = [AVG]

    def __init__(self, suffix, aggregators=None):
        if aggregators is None:
            aggregators = self.DEFAULT_AGGREGATORS
        self.name = None  # set when prefix is set
        self.aggs = tuple(sorted(agg.name for agg in aggregators))
        self.suffix = suffix
        self._values = []  # list of unpolled values

    def manage(self, prefix):
        """Called by :class:`MetricManager` when this metric is registered."""
        if self.name is not None:
            raise MetricRegistrationError("Metric %s%s already registered"
                                          " with a MetricManager." %
                                          (prefix, self.suffix))
        self.name = prefix + self.suffix

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


class TimerAlreadyStartedError(Exception):
    pass


class Timer(Metric):
    """A metric that records time spent on operations.

    Examples:

    >>> mm = MetricManager('vumi.worker0.')
    >>> my_timer = mm.register(Timer('hard.work'))

    Using the timer as a context manager:

    >>> with my_timer:
    >>>     process_data()

    Or equivalently using .start() and stop() directly:

    >>> my_timer.start()
    >>> try:
    >>>     process_other_data()
    >>> finally:
    >>>     my_timer.stop()
    """

    #: Default aggregators are [:data:`AVG`]
    DEFAULT_AGGREGATORS = [AVG]

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
        self.set(duration)


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

    def __init__(self, callback):
        self.callback = callback
        self.queue_name = self.routing_key

    def consume_message(self, vumi_message):
        msg = MetricMessage.from_dict(vumi_message.payload)
        for metric_name, aggregators, values in msg.datapoints():
            self.callback(metric_name, aggregators, values)
