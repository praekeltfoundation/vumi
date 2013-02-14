# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

import copy
import warnings

from twisted.internet.defer import maybeDeferred
from twisted.python import log

from vumi.config import ConfigText, ConfigInt, ConfigDict
from vumi.worker_base import BaseWorker
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage


SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_RESUME = TransportUserMessage.SESSION_RESUME


class ApplicationConfig(BaseWorker.CONFIG_CLASS):
    """Base config definition for applications.

    You should subclass this and add application-specific fields.
    """

    # We override amqp_prefetch_count to set a different default.
    amqp_prefetch_count = ConfigInt(
        "The number of messages fetched concurrently from each AMQP queue"
        " by each worker instance.",
        default=20, static=True)
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

    Messages sent via :meth:`send_to` pass optional additional data
    from configuration to the TransportUserMessage constructor, based
    on the tag parameter passed to send_to. This usually contains
    information useful for routing the message.

    An example :meth:`send_to` configuration might look like::

      - send_to:
        - default:
          transport_name: sms_transport

    Currently 'transport_name' **must** be defined for each send_to
    section since all existing dispatchers rely on this for routing
    outbound messages.

    The available tags are defined by the :attr:`SEND_TO_TAGS` class
    attribute. Sub-classes must override this attribute with a set of
    tag names if they wish to use :meth:`send_to`. If applications
    have only a single tag, it is suggested to name that tag `default`
    (this makes calling `send_to` easier since the value of the tag
    parameter may be omitted).

    By default :attr:`SEND_TO_TAGS` is empty and all calls to
    :meth:`send_to` will fail (this is to make it easy to identify
    which tags an application requires `send_to` configuration for).
    """

    transport_name = None
    start_message_consumer = True

    CONFIG_CLASS = ApplicationConfig
    SEND_TO_TAGS = frozenset([])

    def _worker_specific_setup(self):
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
        return self.setup_application()

    def _validate_config(self):
        # TODO: Figure out what the future of .validate_config() is going to be
        #       once everything uses config objects. It's handy to be able to
        #       do complex validation, but maybe that belongs on the config
        #       object instead?

        # This needs to happen earlier than _worker_specific_setup() allows.
        super(ApplicationWorker, self)._validate_config()
        config = self.get_static_config()
        self.transport_name = config.transport_name
        for tag in self.SEND_TO_TAGS:
            if tag not in config.send_to:
                raise ConfigError("No configuration for send_to tag %r but"
                                  " at least a transport_name is required."
                                  % (tag,))
            if 'transport_name' not in config.send_to[tag]:
                raise ConfigError("The configuration for send_to tag %r must"
                                  " contain a transport_name." % (tag,))

    def _worker_specific_teardown(self):
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
        publisher = self._connectors[self.transport_name]
        return publisher.publish_outbound(message, endpoint_name=endpoint_name)

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        endpoint_name = original_message.get_routing_endpoint()
        return self._publish_message(reply, endpoint_name=endpoint_name)

    def reply_to_group(self, original_message, content, continue_session=True,
                       **kws):
        reply = original_message.reply_group(content, continue_session, **kws)
        return self._publish_message(reply)

    def send_to(self, to_addr, content, tag='default', **kw):
        # FIXME: Do we do the right stuff here?
        if tag not in self.SEND_TO_TAGS:
            raise ValueError("Tag %r not defined in SEND_TO_TAGS" % (tag,))
        options = copy.deepcopy(self.get_static_config().send_to[tag])
        options.update(kw)
        msg = TransportUserMessage.send(to_addr, content, **options)
        return self._publish_message(msg)

    def _check_for_deprecated_method(self, method_name):
        # XXX: Is there a better way to do this?
        my_stp = getattr(type(self), method_name)
        base_stp = getattr(ApplicationWorker, method_name)
        if my_stp == base_stp:
            return False
        warnings.warn(
            "%s() is deprecated. Use connectors and endpoints instead." % (
                method_name,), category=DeprecationWarning)
        return True

    def _setup_deprecated_attrs(self, endpoint_name, connector):
        def setval(suffix, value):
            setattr(self, '%s_%s' % (endpoint_name, suffix), value)
        setval('publisher', connector._publishers['outbound'])
        setval('consumer', connector._consumers['inbound'])
        setval('event_consumer', connector._consumers['event'])

    def setup_connectors(self):
        if self._check_for_deprecated_method('_setup_transport_publisher'):
            return self._setup_transport_publisher()

        d = self.setup_ri_connector(self.transport_name)

        def cb(connector):
            connector.set_inbound_handler(self.dispatch_user_message)
            connector.set_event_handler(self.dispatch_event)
            self._setup_deprecated_attrs('transport', connector)
            return connector

        return d.addCallback(cb)

    def setup_transport_connection(self, endpoint_name, transport_name,
                                   message_consumer, event_consumer):
        warnings.warn(
            "setup_transport_connection() is deprecated. Use connectors and"
            " endpoints instead.", category=DeprecationWarning)

        d = self.setup_ri_connector(transport_name)

        def cb(connector):
            connector.set_inbound_handler(message_consumer)
            connector.set_event_handler(event_consumer)
            self._setup_deprecated_attrs(endpoint_name, connector)
            return connector

        return d.addCallback(cb)

    def _setup_unpause(self):
        if any([
                self._check_for_deprecated_method('_setup_transport_consumer'),
                self._check_for_deprecated_method('_setup_event_consumer')]):
            d = maybeDeferred(self._setup_transport_consumer)
            d.addCallback(lambda r: maybeDeferred(self._setup_event_consumer))
            return d
        return self._connectors[self.transport_name].unpause()

    def _setup_transport_publisher(self):
        return self.setup_transport_connection(
            'transport', self.get_static_config().transport_name,
            self.dispatch_user_message, self.dispatch_event)

    def _setup_transport_consumer(self):
        return self.transport_consumer.unpause()

    def _setup_event_consumer(self):
        return self.transport_event_consumer.unpause()
