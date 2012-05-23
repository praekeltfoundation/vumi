Implementing your own middleware
================================

.. py:module:: vumi.middleware

A middleware class provides four handler functions, one for processing
each of the four kinds of messages transports, applications and
dispatchers typically send and receive (i.e. inbound user messages,
outbound user messages, event messages and failure messages).

Although transport and application middleware potentially both provide
the same sets of handlers, the two make use of them in slightly
different ways. Inbound messages and events are published by
transports but consumed by applications while outbound messages are
opposite. Failure messages are not seen by applications at all and are
allowed only so that certain middleware may be used on both transports
and applications. Dispatchers both consume and publish all kinds of
messages except failure messages.

Middleware is required to have the same interface as the
:class:`BaseMiddleware` class which is described below. Two
subclasses, :class:`TransportMiddleware` and
:class:`ApplicationMiddleware`, are provided but subclassing from
these is just a hint as to whether a piece of middleware is intended
for use on transports or applications (middleware for use on both or
for dispatchers may inherit from :class:`BaseMiddleware`). The two
subclasses provide identical interfaces and no extra functionality.

.. autoclass:: BaseMiddleware
   :member-order: bysource
   :members:

Example of a simple middleware implementation from
:mod:`vumi.middleware.logging`:

.. literalinclude:: /../vumi/middleware/logging.py
   :pyobject: LoggingMiddleware


How your middleware is used inside Vumi
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

While writing complex middleware, it may help to understand how a
middleware class is used by Vumi transports and applications.

When a transport or application is started a list of middleware to
load is read from the configuration. An instance of each piece of
middleware is created and then :meth:`setup_middleware` is called on
each middleware object in order. If any call to
:meth:`setup_middleware` returns a :class:`Deferred`, setup will
continue after the deferred has completed.

Once the middleware has been setup it is combined into a
:class:`MiddlewareStack`. A middleware stack has two important methods
:meth:`apply_consume` and :meth:`apply_publish` The former is used
when a message is being consumed and applies the appropriate handlers
in the order listed in the configuration file. The latter is used when
a message is being published and applies the handlers in the
*reverse* order.
