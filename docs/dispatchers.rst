Dispatchers
===========

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

.. TODO::

   A drawing showing a bunch of transports and applications connected
   to a dispatcher with endpoint names on all the links.

.. TODO::

   Mention routing classes.
   Example configuration.

Further reading:

.. toctree::
    :maxdepth: 1

    dispatchers/builtin.rst
    dispatchers/implementing.rst
