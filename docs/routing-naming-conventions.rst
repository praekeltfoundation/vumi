Routing Naming Conventions
==========================

Transports
----------

Transports use the following routing key convention:

* `<transport_name>.inbound` for sending messages from users (to vumi
  applications).
* `<transport_name>.outbound` for receiving messages to send to users
  (from vumi applications).
* `<transport_name>.event` for sending message-related events
  (e.g. acknowledgements, delivery reports) to vumi applications.
* `<transport_name>.failures` for sending failed messages to failure
  workers.

Transports use the `vumi` exchange (which is a `direct` exchange).


Metrics
-------

The routing keys used by metrics workers are detailed in the table
below. Exchanges are `direct` unless otherwise specified.

.. csv-table:: Routing Naming Conventions
   :header: "Component", "Consumer / Producer", "Exchange", "Exch. Type", "Queue Name", "Routing Key", "Notes"

   "MetricTimeBucket"
   "", "Consumer", "vumi.metrics", "", "vumi.metrics", "vumi.metrics"
   "", "Publisher", "vumi.metrics.buckets", "", "bucket.<number>", "bucket.<number>"

   "MetricAggregator"
   "", "Consumer", "vumi.metrics.buckets", "", "bucket.<number>", "bucket.<number>"
   "", "Publisher", "vumi.metrics.aggregates", "", "vumi.metrics.aggregates", "vumi.metrics.aggregates"

   "GraphiteMetricsCollector"
   "", "Consumer", "vumi.metrics.aggregates", "",	"vumi.metrics.aggregates", "vumi.metrics.aggregates"
   "", "Publisher",	"graphite", "topic", "n/a", "<metric name>"
