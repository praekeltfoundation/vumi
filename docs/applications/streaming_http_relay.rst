Streaming HTTP Relay
====================

Provides an HTTP API for sending and receiving vumi messages.

When configured for streaming (by not setting :py:attr:`push_message_url` or :py:attr:`push_event_url` configuration fields), clients may make HTTP GET requests to the configured URL to receive a stream of JSON message objects. If :py:attr:`push_message_url` and :py:attr:`push_event_url` are configured, the application worker will POST vumi messages to those URLs.

Outbound messages may be sent by making HTTP PUT (or POST) requests to the streaming URL.

Streaming HTTP Relay
^^^^^^^^^^^^^^^^^^^^

.. py:module:: vumi.application.streaming_http_relay

.. autoclass:: StreamingHTTPRelayConfig

.. autoclass:: StreamingHTTPRelayWorker
