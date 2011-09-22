Routing Naming Conventions
==========================

Exchanges are `direct` unless otherwise specified.

.. csv-table:: Routing Naming Conventions
   :header: "Component", "Consumer / Producer", "Exchange", "Exch. Type", "Queue Name", "Routing Key", "Notes"

   "SMPP Transport"
   "", "Consumer", "vumi", "", "sms.outbound.clickatell", "sms.outbound.clickatell", ""
   "", "Publisher", "vumi", "", "n/a", "sms.inbound.clickatell.<msisdn>", ""
   "", "Publisher", "vumi", "", "n/a", "sms.receipt.clickatell", ""

   "Opera XML-RPC Transport"
   "", "Consumer", "vumi", "", "sms.outbound.opera", "sms.outbound.opera", ""
   "", "Publisher", "vumi", "", "n/a", "sms.inbound.opera.<msisdn>", ""
   "", "Publisher", "vumi", "", "n/a", "sms.receipt.opera"

   "XMPP Transport"
   "", "Consumer", "vumi", "", "xmpp.outbound.gtalk.<jid>", "xmpp.outbound.gtalk.<jid>"
   "", "Publisher", "vumi", "", "n/a", "xmpp.inbound.gtalk.<jid>"

   "HTTP api"
   "", "Publisher", "vumi",	"", "n/a", "sms.internal.debatcher"

   "ReceiptWorker"
   "", "Consumer", "vumi", "", "sms.receipt.clickatell", "sms.receipt.clickatell"
   "", "Consumer", "vumi", "", "sms.receipt.opera", "sms.receipt.opera"

   "Debatcher"
   "", "Consumer", "vumi", "", "sms.internal.debatcher", "sms.internal.debatcher"
   "", "Publisher", "vumi", "", "n/a, "sms.outbound.clickatell"

   "KeywordWorker"
   "", "Consumer", "vumi", "", "sms.inbound.clickatell.<msisdn>", "sms.inbound.clickatell.<msisdn>"
   "", "Publisher", "vumi", "", "n/a", "sms.inbound.clickatell.<msisdn>.<keyword>", "dynamically create routing key based on keyword lookup. Fallback to only <msisdn> if not suitable keyword found in database."

   "CampaignWorker"
   "", "Consumer", "vumi", "", "sms.inbound.clickatell.<msisdn>", "sms.inbound.clickatell.<msisdn>"
   "", "Consumer", "vumi", "", "sms.inbound.clickatell.<msisdn>.<keyword>", "sms.inbound.clickatell.<msisdn>.<keyword>"
   "", "Publisher", "vumi", "", "n/a", "sms.outbound.clickatell"

   "Twitter"
   "", "Consumer", "vumi", "", "twitter.outbound.vumiapp", "twitter.outbound.vumiapp"
   "", "Publisher", "vumi", "", "n/a", "twitter.inbound.vumiapp"

   "Gsm transport"
   "", "Consumer", "vumi", "", "gsm.outbound.<msisdn>", "gsm.outbound.<msisdn>"
   "", "Publisher", "vumi", "", "n/a", "gsm.inbound.<msisdn>"

   "Vas2Nets transport"
   "", "Consumer", "vumi", "", "sms.outbound.vas2nets", "sms.outbound.vas2nets"
   "", "Publisher", "vumi", "", "n/a", "sms.inbound.vas2nets.<msisdn>"
   "", "Publisher", "vumi", "", "n/a", "sms.receipt.vas2nets"

   "MetricTimeBucket"
   "", "Consumer", "vumi.metrics", "", "vumi.metrics", "vumi.metrics"
   "", "Publisher", "vumi.metrics.buckets", "", "bucket.<number>", "bucket.<number>"

   "MetricAggregator"
   "", "Consumer", "vumi.metrics.buckets", "", "bucket.<number>", "bucket.<number>"
   "", "Publisher", "vumi.metrics.aggregates", "", "vumi.metrics.aggregates", "vumi.metrics.aggregates"

   "GraphiteMetricsCollector"
   "", "Consumer", "vumi.metrics.aggregates", "",	"vumi.metrics.aggregates", "vumi.metrics.aggregates"
   "", "Publisher",	"graphite", "topic", "n/a", "<metric name>"
