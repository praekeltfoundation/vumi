Builtin middleware
==================

Vumi ships with a small set of generically useful middleware:

.. contents:: Vumi middleware
    :local:


AddressTranslationMiddleware
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Overwrites `to_addr` and `from_addr` values based on a simple
mapping. Useful for debugging and testing.

.. autoclass:: vumi.middleware.address_translator.AddressTranslationMiddleware


LoggingMiddleware
^^^^^^^^^^^^^^^^^

Logs messages, events and failures as they enter or leave a transport.

.. autoclass:: vumi.middleware.logging.LoggingMiddleware


TaggingMiddleware
^^^^^^^^^^^^^^^^^

.. autoclass:: vumi.middleware.tagger.TaggingMiddleware


StoringMiddleware
^^^^^^^^^^^^^^^^^

.. autoclass:: vumi.middleware.message_storing.StoringMiddleware
