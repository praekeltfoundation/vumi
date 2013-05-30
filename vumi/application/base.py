# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

import copy

from twisted.internet.defer import maybeDeferred

from vumi.config import ConfigText, ConfigDict
from vumi.worker import BaseWorker
from vumi import log
from vumi.message import TransportUserMessage


SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_RESUME = TransportUserMessage.SESSION_RESUME


class ApplicationConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for applications.

    You should subclass this and add application-specific fields.
    """

    transport_name = ConfigText(
        "The name this application instance will use to create its queues.",
        required=True, static=True)
    send_to = ConfigDict(
        "'send_to' configuration dict.", default={}, static=True)


class ApplicationWorker(BaseWorker):
    """Base class for an application worker.

    Handles :class:`vumi.message.TransportUserMessage` and
    :class:`vumi.message.TransportEvent` messages.

    Application workers may send outgoing messages using
    :meth:`reply_to` (for replies to incoming messages) or
    :meth:`send_to` (for messages that are not replies).

    :meth:`send_to` can take either an `endpoint` parameter or a deprecated
    `tag` parameter.

    :attr:`ALLOWED_ENDPOINTS` lists the endpoints this application is allowed
    to send messages to using the :meth:`send_to` method. If it is set to
    `None`, any endpoint is allowed.

    (DEPRECATED) Messages sent via :meth:`send_to` pass optional additional
    data from configuration to the TransportUserMessage constructor, based on
    the tag parameter passed to send_to. This usually contains information
    useful for routing the message.

    An example :meth:`send_to` configuration might look like::

      - send_to:
        - default:
          transport_name: sms_transport

    NOTE: If you are using non-endpoint routing, 'transport_name' **must** be
    defined for each send_to section since dispatchers rely on this for routing
    outbound messages.

    (DEPRECATED) The available tags are defined by the :attr:`SEND_TO_TAGS`
    class attribute. Subclasses must override this attribute with a set of tag
    names if they wish to use :meth:`send_to`. If applications have only a
    single tag, it is suggested to name that tag `default` (this makes calling
    `send_to` easier since the value of the tag parameter may be omitted).

    (DEPRECATED) By default :attr:`SEND_TO_TAGS` is empty and all calls to
    :meth:`send_to` will fail (this is to make it easy to identify which tags
    an application requires `send_to` configuration for).
    """

    transport_name = None
    UNPAUSE_CONNECTORS = True

    CONFIG_CLASS = ApplicationConfig
    SEND_TO_TAGS = None
    ALLOWED_ENDPOINTS = frozenset(['default'])

    def _validate_config(self):
        config = self.get_static_config()
        self.transport_name = config.transport_name
        for tag in (self.SEND_TO_TAGS or []):
            if tag not in config.send_to:
                log.warning(
                    "No configuration for send_to tag %r. If you are using"
                    " (deprecated) non-endpoint routing, at least a"
                    " transport_name is required." % (tag,))
            elif 'transport_name' not in config.send_to[tag]:
                log.warning(
                    "No transport_name configured for send_to tag %r. If you"
                    " are using (deprecated) non-endpoint routing, one is"
                    " required." % (tag,))
        self.validate_config()

    def setup_connectors(self):
        d = self.setup_ri_connector(self.transport_name)

        def cb(connector):
            connector.set_inbound_handler(self.dispatch_user_message)
            connector.set_event_handler(self.dispatch_event)
            return connector

        return d.addCallback(cb)

    def setup_worker(self):
        """
        Set up basic application worker stuff.

        You shouldn't have to override this in subclasses.
        """
        self._event_handlers = {
            'ack': self.consume_ack,
            'nack': self.consume_nack,
            'delivery_report': self.consume_delivery_report,
        }
        self._session_handlers = {
            SESSION_NEW: self.new_session,
            SESSION_CLOSE: self.close_session,
        }
        d = maybeDeferred(self.setup_application)

        if self.UNPAUSE_CONNECTORS:
            d.addCallback(lambda r: self.unpause_connectors())

        return d

    def teardown_worker(self):
        self.pause_connectors()
        return self.teardown_application()

    def setup_application(self):
        """
        All application specific setup should happen in here.

        Subclasses should override this method to perform extra setup.
        """
        pass

    def teardown_application(self):
        """
        Clean-up of setup done in setup_application should happen here.
        """
        pass

    def _dispatch_event_raw(self, event):
        event_type = event.get('event_type')
        handler = self._event_handlers.get(event_type,
                                           self.consume_unknown_event)
        return handler(event)

    def dispatch_event(self, event):
        """Dispatch to event_type specific handlers."""
        return self._dispatch_event_raw(event)

    def consume_unknown_event(self, event):
        log.msg("Unknown event type in message %r" % (event,))

    def consume_ack(self, event):
        """Handle an ack message."""
        pass

    def consume_nack(self, event):
        """Handle a nack message"""
        pass

    def consume_delivery_report(self, event):
        """Handle a delivery report."""
        pass

    def _dispatch_user_message_raw(self, message):
        session_event = message.get('session_event')
        handler = self._session_handlers.get(session_event,
                                             self.consume_user_message)
        return handler(message)

    def dispatch_user_message(self, message):
        """Dispatch user messages to handler."""
        return self._dispatch_user_message_raw(message)

    def consume_user_message(self, message):
        """Respond to user message."""
        pass

    def new_session(self, message):
        """Respond to a new session.

        Defaults to calling consume_user_message.
        """
        return self.consume_user_message(message)

    def close_session(self, message):
        """Close a session.

        The .reply_to() method should not be called when the session is closed.
        """
        pass

    def _publish_message(self, message, endpoint_name=None):
        publisher = self.connectors[self.transport_name]
        return publisher.publish_outbound(message, endpoint_name=endpoint_name)

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        endpoint_name = original_message.get_routing_endpoint()
        return self._publish_message(reply, endpoint_name=endpoint_name)

    def reply_to_group(self, original_message, content, continue_session=True,
                       **kws):
        reply = original_message.reply_group(content, continue_session, **kws)
        endpoint_name = original_message.get_routing_endpoint()
        return self._publish_message(reply, endpoint_name=endpoint_name)

    def send_to(self, to_addr, content, endpoint=None, **kw):
        if endpoint is None:
            endpoint = 'default'

        if (self.ALLOWED_ENDPOINTS is not None
                and endpoint not in self.ALLOWED_ENDPOINTS):
            raise ValueError("Endpoint %r not defined in ALLOWED_ENDPOINTS" % (
                endpoint,))

        options = copy.deepcopy(
            self.get_static_config().send_to.get(endpoint, {}))
        options.update(kw)
        msg = TransportUserMessage.send(to_addr, content, **options)
        return self._publish_message(msg, endpoint_name=endpoint)
