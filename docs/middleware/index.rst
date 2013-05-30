Middleware
==========

Middleware provides additional functionality that can be attached to
any existing transport, application or dispatcher worker. For
example, middleware could log inbound and outbound messages, store
delivery reports in a database or modify a message.

Attaching middleware to your worker is fairly straight forward. Just
extend your YAML configuration file with lines like::

    middleware:
        - mw1: vumi.middleware.LoggingMiddleware

    mw1:
        log_level: info

The `middleware` section contains a list of middleware items. Each
item consists of a `name` (e.g. `mw1`) for that middleware instance
and a `class` (e.g. :class:`vumi.middleware.LoggingMiddleware`)
which is the full Python path to the class implementing the
middleware. A `name` can be any string that doesn't clash with another
top-level configuration option -- it's just used to look up the
configuration for the middleware itself.

If a middleware class doesn't require any additional parameters, the
configuration section (i.e. the `mw1: debug_level ...` in the example
above) may simply be omitted.

Multiple layers of middleware may be specified as follows::

    middleware:
        - mw1: vumi.middleware.LoggingMiddleware
        - mw2: mypackage.CustomMiddleware

You can think of the layers of middleware sitting on top of the
underlying transport or application worker. Messages being consumed by
the worker enter from the top and are processed by the middleware in
the order you have defined them and eventually reach the worker at the
bottom. Messages published by the worker start at the bottom and
travel up through the layers of middleware before finally exiting the
middleware at the top.

Further reading:

.. toctree::
    :maxdepth: 1

    builtin.rst
    implementing.rst
