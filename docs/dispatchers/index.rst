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

* A single application that sends and receives SMSes in multiple
  countries using a different transport in each.

* A single SMPP transport that sends and receives SMSes on behalf of
  multiple applications.

* Multiple applications that all send and receive SMSes in multiple
  countries using a shared set of SMPP transports.

Vumi provides a pluggable dispatch worker :class:`BaseDispatchWorker`
that may be extended by much simpler *routing classes* that implement
only the logic for routing messages (see :ref:`routers-section`). The
pluggable dispatcher handles setting up endpoints for all the
transports and application workers the dispatcher communicates with.

.. tikz:: A simple dispatcher configuration. Boxes represent workers. Edges are routing links between workers. Edges are labelled with endpoint names (i.e. transport_names).
   :filename: images/tikz/simple-dispatcher-example.png
   :libs: arrows,shadows,decorations.pathmorphing,shapes,positioning

   \tikzstyle{place}=[double copy shadow,
                      shape=rounded rectangle,
                      thick,
                      font=\scriptsize,
                      inner sep=0pt,
                      outer sep=0.3ex,
                      minimum height=1.5em,
                      minimum width=8em,
                      node distance=8em,
                     ];

   \tikzstyle{link}=[<->,
                     >=stealth,
                     font=\scriptsize,
                     line width=0.2ex,
                     auto,
                     ];

   \definecolor{darkgreen}{rgb}{0,0.5,0};
   \definecolor{darkblue}{rgb}{0,0,0.5};

   \tikzstyle{route}=[sloped,midway,above=0.1em];
   \tikzstyle{transport_name}=[draw=darkgreen];
   \tikzstyle{exposed_name}=[draw=darkblue];
   \tikzstyle{transport}=[draw=darkgreen!50,fill=darkgreen!20]
   \tikzstyle{application}=[draw=darkblue!50,fill=darkblue!20]
   \tikzstyle{dispatcher}=[draw=black!50,fill=black!20]

   \node[place,dispatcher] (dispatcher) {Dispatcher};
   \node[place,transport] (smpp_transport) [above left=of dispatcher] {SMPP Transport};
   \node[place,transport] (xmpp_transport) [below left=of dispatcher] {XMPP Transport};
   \node[place,application] (my_application) [right=of dispatcher] {My Application};

   \draw[link,transport_name] (smpp_transport) to node [route] {smpp\_transport} (dispatcher);
   \draw[link,transport_name] (xmpp_transport) to node [route] {xmpp\_transport} (dispatcher);
   \draw[link,exposed_name] (my_application) to node [route] {my\_application} (dispatcher);

A simple :class:`BaseDispatchWorker` YAML configuration file for the
example above might look like::

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
options. These are described in :doc:`builtin`.


.. _routers-section:

Routers
^^^^^^^

Router classes implement dispatching of inbound and outbound messages
and events. Inbound messages and events come from transports and are
typically dispatched to an application. Outbound messages come from
applications and are typically dispatched to a transport.

Many routers follow a simple pattern:

* `inbound` messages are routed using custom routing logic.
* `events` are routed towards the same application the associated
  message was routed to.
* `outbound` messages that are replies are routed towards the
  transport that the original message came from.
* `outbound` messages that are not replies are routed based on
  additional information provided by the application (in simple setups
  its common for the application to simply provide the name of the
  transport the message should be routed to).

You can read more about the routers Vumi provides and about how to
write your own router class in the following sections:

.. toctree::
    :maxdepth: 1

    builtin.rst
    implementing.rst
