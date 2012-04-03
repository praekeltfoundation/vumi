Implementing your own router
============================

.. py:module:: vumi.dispatchers

A middleware class provides four handler functions, one for processing
each of the four kinds of message transports and applications typical
sent and receive (i.e. inbound user messages, outbound user messages,
event messages and failure messages).

Although transport and application middleware potentially both provide
the same sets of handlers, the two make use of them in slightly
different ways. Inbound messages and events are published by
transports but consumed by applications while outbound messages are
opposite. Failure messages are not seen by applications at all and are
allowed only so that certain middleware may be used on both transports
and applications.

Middleware is required to have the same interface as the
:class:`BaseMiddleware` class which is described below. Two
subclasses, :class:`TransportMiddleware` and
:class:`ApplicationMiddleware`, are provided but subclassing from
these is just a hint as to whether a piece of middleware is intended
for use on transports or applications. The two subclasses provide
identical interfaces and no extra functionality.

.. autoclass:: BaseDispatchRouter
   :member-order: bysource
   :members:

Example of a simple middleware implementation from
:mod:`vumi.dispatcher.base`:

.. literalinclude:: /../vumi/dispatchers/base.py
   :pyobject: SimpleDispatchRouter
