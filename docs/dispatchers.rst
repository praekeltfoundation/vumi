Dispatchers
===========

.. py:module:: vumi.dispatchers

Dispatchers are vumi workers that route messages between sets of
transports and sets of application workers.

Vumi transports and application workers both have a single *endpoint*
on which messages are sent and received (the name of the endpoint is
given by the *transport_name* configuration option). Connecting sets
of transports and applications requires a kind of worker with
*multiple* endpoints. This class of workers is the dispatcher.

Examples of use cases for dispatchers:

* A single application that sends and receives both SMSes and XMPP
  messages.

* A single SMPP transports that sends and receives SMSes on behalf of
  multiple applications.

* Multiple applications that all send and receive SMSes in multiple
  countries using a common set of SMPP transports.

Vumi provides a pluggable dispatch worker :class:`BaseDispatchWorker`
that may be extended by much simpler *routing classes* that implement
only the logic for routing messages (see :ref:`Routers`). The
pluggable dispatcher worker handles setting up endpoints for all the
transports and application workers the dispatcher communicates with.

A simple :class:`BaseDispatchWorker` YAML configuration file for the
first example above might look like::

  # dispatcher config

  router_class: vumi.dispatchers.SimpleDispatchRouter

  transport_names:
    - smpp_transport
    - xmpp_transport

  exposed_names:
    - my_application

  # router config

  route_mappings:
    smpp_transport: my_application
    xmpp_transport: my_application

The `router_class`, `transport_names` and `exposed_names` sections are
all configuration for the :class:`BaseDispatchWorker` itself and will
be present in all dispatcher configurations:

* `router_class` gives the full Python path to the class
  implementing the routing logic.

* `transport_names` is the list of transport endpoints the dispatcher
  should receive and publish messages on.

* `exposed_names` is the list of application endpoints the dispatcher
  should receive and publish messages on.

The last section, `routing_mappings`, is specific to the router class
used (i.e. :class:`vumi.dispatchers.SimpleDispatchRouter`). It lists
the application endpoint that messages and events from each transport
should be sent to. In this simple example message from both transports
are sent to the same application worker.

Other router classes will have different router configuration
options. These are described in :ref:`dispatchers/builtin.rst`.


Routers
^^^^^^^

.. TODO::

   A drawing showing a bunch of transports and applications connected
   to a dispatcher with endpoint names on all the links.

Further reading:

.. toctree::
    :maxdepth: 1

    dispatchers/builtin.rst
    dispatchers/implementing.rst
