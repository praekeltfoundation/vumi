# -*- test-case-name: vumi.application.tests.test_base -*-

"""Basic tools for building a vumi ApplicationWorker."""

import copy
import json

from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.errors import ConfigError
from vumi.message import TransportUserMessage, TransportEvent
from vumi.session import getVumiSession, delVumiSession, TraversedDecisionTree


SESSION_NEW = TransportUserMessage.SESSION_NEW
SESSION_CLOSE = TransportUserMessage.SESSION_CLOSE
SESSION_RESUME = TransportUserMessage.SESSION_RESUME


class ApplicationWorker(Worker):
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

    SEND_TO_TAGS = frozenset([])

    @inlineCallbacks
    def startWorker(self):
        log.msg('Starting a %s worker with config: %s'
                % (self.__class__.__name__, self.config))
        self._consumers = []
        self._validate_config()
        self.transport_name = self.config['transport_name']
        self.send_to_options = self.config.get('send_to', {})

        self._event_handlers = {
            'ack': self.consume_ack,
            'delivery_report': self.consume_delivery_report,
            }
        self._session_handlers = {
            SESSION_NEW: self.new_session,
            SESSION_CLOSE: self.close_session,
            }

        yield self._setup_transport_publisher()

        yield self.setup_application()

        if self.start_message_consumer:
            yield self._setup_transport_consumer()
            yield self._setup_event_consumer()

    @inlineCallbacks
    def stopWorker(self):
        while self._consumers:
            consumer = self._consumers.pop()
            yield consumer.stop()
        yield self.teardown_application()

    def _validate_config(self):
        if 'transport_name' not in self.config:
            raise ConfigError("Missing 'transport_name' field in config.")
        send_to_options = self.config.get('send_to', {})
        for tag in self.SEND_TO_TAGS:
            if tag not in send_to_options:
                raise ConfigError("No configuration for send_to tag %r but"
                                  " at least a transport_name is required."
                                  % (tag,))
            if 'transport_name' not in send_to_options[tag]:
                raise ConfigError("The configuration for send_to tag %r must"
                                  " contain a transport_name." % (tag,))

        return self.validate_config()

    def validate_config(self):
        """
        Application-specific config validation happens in here.

        Subclasses may override this method to perform extra config validation.
        """
        pass

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

    def dispatch_event(self, event):
        """Dispatch to event_type specific handlers."""
        event_type = event.get('event_type')
        handler = self._event_handlers.get(event_type,
                                           self.consume_unknown_event)
        return handler(event)

    def consume_unknown_event(self, event):
        log.msg("Unknown event type in message %r" % (event,))

    def consume_ack(self, event):
        """Handle an ack message."""
        pass

    def consume_delivery_report(self, event):
        """Handle a delivery report."""
        pass

    def dispatch_user_message(self, message):
        """Dispatch user messages to handler."""
        session_event = message.get('session_event')
        handler = self._session_handlers.get(session_event,
                                             self.consume_user_message)
        return handler(message)

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

    def _publish_message(self, message):
        self.transport_publisher.publish_message(message)
        return message

    def reply_to(self, original_message, content, continue_session=True,
                 **kws):
        reply = original_message.reply(content, continue_session, **kws)
        return self._publish_message(reply)

    def reply_to_group(self, original_message, content, continue_session=True,
                       **kws):
        reply = original_message.reply_group(content, continue_session, **kws)
        return self._publish_message(reply)

    def send_to(self, to_addr, content, tag='default', **kw):
        if tag not in self.SEND_TO_TAGS:
            raise ValueError("Tag %r not defined in SEND_TO_TAGS" % (tag,))
        options = copy.deepcopy(self.send_to_options[tag])
        options.update(kw)
        msg = TransportUserMessage.send(to_addr, content, **options)
        return self._publish_message(msg)

    @inlineCallbacks
    def _setup_transport_publisher(self):
        self.transport_publisher = yield self.publish_to(
            '%(transport_name)s.outbound' % self.config)

    @inlineCallbacks
    def _setup_transport_consumer(self):
        self.transport_consumer = yield self.consume(
            '%(transport_name)s.inbound' % self.config,
            self.dispatch_user_message,
            message_class=TransportUserMessage)
        self._consumers.append(self.transport_consumer)

    @inlineCallbacks
    def _setup_event_consumer(self):
        self.transport_event_consumer = yield self.consume(
            '%(transport_name)s.event' % self.config,
            self.dispatch_event,
            message_class=TransportEvent)
        self._consumers.append(self.transport_event_consumer)


class DecisionTreeWorker(ApplicationWorker):

    @inlineCallbacks
    def startWorker(self):
        self.worker_name = self.config['worker_name']
        #self.yaml_template = None
        #self.r_server = FakeRedis()
        #self.r_server = None
        yield super(DecisionTreeWorker, self).startWorker()

    @inlineCallbacks
    def stopWorker(self):
        yield super(DecisionTreeWorker, self).stopWorker()

    @inlineCallbacks
    def consume_user_message(self, msg):
        try:
            response = ''
            continue_session = False
            if True:
                if not self.yaml_template:
                    raise Exception("yaml_template is missing")
                    #self.set_yaml_template(self.test_yaml)
                sess = self.get_session(msg.user())
                if not sess.get_decision_tree().is_started():
                    # TODO check this corresponds to session_event = new
                    sess.get_decision_tree().start()
                    response += sess.get_decision_tree().question()
                    continue_session = True
                else:
                    # TODO check this corresponds to session_event = resume
                    sess.get_decision_tree().answer(msg.payload['content'])
                    if not sess.get_decision_tree().is_completed():
                        response += sess.get_decision_tree().question()
                        continue_session = True
                    response += sess.get_decision_tree().finish() or ''
                    if sess.get_decision_tree().is_completed():
                        self.post_result(json.dumps(
                            sess.get_decision_tree().get_data()))
                        sess.delete()
                sess.save()
        except Exception, e:
            print e
        user_id = msg.user()
        self.reply_to(msg, response, continue_session)
        yield None

    def set_yaml_template(self, yaml_template):
        self.yaml_template = yaml_template

    def set_data_url(self, data_source):
        self.data_url = data_source

    def set_post_url(self, post_source):
        self.post_url = post_source

    def post_result(self, result):
        # TODO need actual post but
        # just need this to override in testing for now
        #print self.post_url
        #print result
        pass

    def call_for_json(self):
        # TODO need actual retrieval but
        # just need this to override in testing for now
        return '{}'

    def consume_message(self, message):
        # TODO: Eep! This code is broken!
        log.msg("session message %s consumed by %s" % (
            json.dumps(dictionary), self.__class__.__name__))
        #dictionary = message.get('short_message')

    def get_session(self, MSISDN):
        #sess = getVumiSession(self.r_server,
                              #self.routing_key + '.' + MSISDN)
        sess = getVumiSession(self.r_server,
                self.transport_name + '.' + MSISDN)
        if not sess.get_decision_tree():
            sess.set_decision_tree(self.setup_new_decision_tree(MSISDN))
        return sess

    def del_session(self, MSISDN):
        return delVumiSession(self.r_server, MSISDN)

    def setup_new_decision_tree(self, MSISDN, **kwargs):
        decision_tree = TraversedDecisionTree()
        yaml_template = self.yaml_template
        decision_tree.load_yaml_template(yaml_template)
        self.set_data_url(decision_tree.get_data_source())
        self.set_post_url(decision_tree.get_post_source())
        if self.data_url.get('url'):
            json_string = self.call_for_json()
            decision_tree.load_json_data(json_string)
        else:
            decision_tree.load_dummy_data()
        return decision_tree
