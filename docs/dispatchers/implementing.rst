Implementing your own router
============================

.. py:module:: vumi.dispatchers

A routing class publishes message on behalf of a dispatch worker. To
do so it must provide three dispatch functions -- one for inbound user
messages, one for outbound user messages and one for events
(e.g. delivery reports and acknowledgements). Failure messages are not
routed via dispatchers and are typically sent directly to a failure
worker. The receiving of messages and events is handled by the
dispatcher itself.

A dispatcher provides three dictionaires of publishers as attributes:

* `exposed_publisher` -- publishers for sending inbound user messages
  to applications attached to the dispatcher.
* `exposed_event_publisher` -- publishers for sending events to
  applications.
* `transport_publisher` -- publishers for sending outbound user
   messages to transports attached to the dispatcher.

Each of these dictionaries is keyed by *endpoint* name. The keys for
`exposed_publisher` and `exposed_event_publisher` are the endpoints
listed in the `exposed_names` configuration option passed to the
dispatcher. The keys for `transport_publisher` are the endpoints
listed in the `transport_names` configuration option. Routing classes
publish messages by calling the :meth:`publish_message` method on one
of the publishers in these three dictionaries.

Routers are required to have the same interface as the
:class:`BaseDipatcherRouter` class which is described below.

.. autoclass:: BaseDispatchRouter
   :member-order: bysource
   :members:

Example of a simple router implementation from
:mod:`vumi.dispatcher.base`:

.. literalinclude:: /../vumi/dispatchers/base.py
   :pyobject: SimpleDispatchRouter
