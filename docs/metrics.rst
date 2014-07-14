Metrics
=======

.. module:: vumi.blinkenlights.metrics

Metrics are a means for workers to publish statistics about their
operations for real-time plotting and later analysis. Vumi provides
built-in support for publishing metric values to Carbon (the storage
engine used by `Graphite`_).


Using metrics from a worker
---------------------------

The set of metrics a worker wishes to plublish are managed via a
:class:`MetricManager` instance. The manager acts both as a container
for the set of metrics and the publisher that pushes metric values out
via AMQP.

Example::

   class MyWorker(Worker):

       def startWorker(self, config):
           self.metrics = yield self.start_publisher(MetricManager,
                                                     "myworker.")
           self.metrics.register(Metric("a.value"))
           self.metrics.register(Count("a.count"))

In the example above a :class:`MetricManager` publisher is
started. All its metric names with be prefixed with `myworker.`. Two
metrics are registered -- `a.value` whose values will be averaged and
`a.count` whose values will be summed. Later, the worker may set the
metric values like so::

   self.metrics["a.value"].set(1.23)
   self.metrics["a.count"].inc()

.. autoclass:: MetricManager
    :members:

Metrics
-------

A :class:`Metric` object publishes floating point values under a
metric `name`. The name is created by combining the `prefix` from a
metric manager with the `suffix` provided when the metric is
constructed. A metric may only be registered with a single
:class:`MetricManager`.

When a metric value is `set` the value is stored in an internal list
until the :class:`MetricManager` polls the metric for values and
publishes them.

A metric includes a list of aggregation functions to request that
the metric aggregation workers apply (see later sections). Each metric
class has a default list of aggregators but this may be overridden when
a metric is created.

.. autoclass:: Metric
    :members:

.. autoclass:: Count
    :members:
    :show-inheritance:

.. autoclass:: Timer
    :members:
    :show-inheritance:


Aggregation functions
---------------------

Metrics declare which aggregation functions they wish to have applied
but the actual aggregation is performed by aggregation workers. All
values sent during an aggregation interval are aggregated into a
single new value.

Aggregation fulfils two primary purposes:

* To combine metrics from multiple workers into a single aggregated
  value (e.g. to determine the average time taken or total number of
  requests processed across multiple works).
* To produce metric values at fixed time intervals (as is commonly
  required by metric storage backends such as `Graphite`_ and `RRD Tool
  <http://oss.oetiker.ch/rrdtool/>`_).

The aggregation functions currently available are:

* :const:`SUM` -- returns the sum of the supplied values.
* :const:`AVG` -- returns the arithmetic mean of the supplied values.
* :const:`MIN` -- returns the minimum value.
* :const:`MAX` -- returns the maximum value.

All aggregation functions return the value 0.0 if there are no values
to aggregate.

New aggregators may be created by instantiating the :class:`Aggregator`
class.

.. note::

   The aggregator must be instantiated in both the process that
   generates the metric (usually a worker) and the process that
   performs the aggregation (usually an aggregation worker).

.. autoclass:: Aggregator
   :members:


Metrics aggregation system
--------------------------

The metric aggregation system consists of :class:`MetricTimeBucket`
and :class:`MetricAggregator` workers.

The :class:`MetricTimeBucket` workers pull metrics messages from the
`vumi.metrics` exchange and publish them on the `vumi.metrics.buckets`
exchange under a routing key specific to the :class:`MetricAggregator`
which should process them. Once sufficient time has passed for all
metrics for a specific time period (a.k.a. time bucket) to have
arrived at the aggregator, the requested aggregation functions are
applied and the resulting aggregated metrics are published to the
`vumi.metrics.aggregates` exchange.

A typical metric aggregation setup might consist of the following
workers:
* 2 :class:`MetricTimeBucket` workers
* 3 :class:`MetricAggregator` workers
* a final metric collector, e.g. :class:`GraphiteMetricsCollector`.

A shell script to start-up such a setup might be::

  #!/bin/bash
  BUCKET_OPTS="--worker_class=vumi.blinkenlights.MetricTimeBucket \
  --set-option=buckets:3 --set-option=bucket_size:5"

  AGGREGATOR_OPTS="--worker_class=vumi.blinkenlights.MetricAggregator \
  --set-option=bucket_size:5"

  GRAPHITE_OPTS="--worker_class=vumi.blinkenlights.GraphiteMetricsCollector"

  twistd -n vumi_worker $BUCKET_OPTS &
  twistd -n vumi_worker $BUCKET_OPTS &

  twistd -n vumi_worker $AGGREGATOR_OPTS --set-option=bucket:0 &
  twistd -n vumi_worker $AGGREGATOR_OPTS --set-option=bucket:1 &
  twistd -n vumi_worker $AGGREGATOR_OPTS --set-option=bucket:2 &

  twistd -n vumi_worker $GRAPHITE_OPTS &


Publishing to Graphite
----------------------

The :class:`GraphiteMetricsCollector` collects aggregate metrics
(produced by the metrics aggregators) and publishes them to Carbon
(Graphite's metric collection package) over AMQP.

You can read about installing a configuring Graphite at
`<http://graphite.wikidot.com>`_ but at the very least you will have to enable
AMQP support by setting::

  [cache]
  ENABLE_AMQP = True
  AMQP_METRIC_NAME_IN_BODY = False

in Carbon's configuration file.

If you have the metric aggregation system configured as in the section
above you can start Carbon cache using::

  carbon-cache.py --config <config file> --debug start

.. _Graphite: http://graphite.wikidot.com/
