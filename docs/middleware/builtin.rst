Builtin middleware
==================

Vumi ships with a small set of generically useful middleware:

.. contents:: Vumi middleware
    :local:

.. py:module:: vumi.middleware

AddressTranslationMiddleware
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Overwrites `to_addr` and `from_addr` values based on a simple
mapping. Useful for debugging and testing.

.. autoclass:: AddressTranslationMiddleware


LoggingMiddleware
^^^^^^^^^^^^^^^^^

Logs messages, events and failures as they enter or leave a transport.

.. autoclass:: LoggingMiddleware


TaggingMiddleware
^^^^^^^^^^^^^^^^^

.. autoclass:: TaggingMiddleware


StoringMiddleware
^^^^^^^^^^^^^^^^^

.. autoclass:: StoringMiddleware
