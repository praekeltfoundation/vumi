import os

from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.message import TransportUserMessage, TransportEvent


def get_timeout():
    # Look up the timeout in an environment variable and use a default of 5
    # seconds if there isn't one there.
    timeout_str = os.environ.get('VUMI_TEST_TIMEOUT', '5')
    return float(timeout_str)


class VumiTestCase(TestCase):
    timeout = get_timeout()

    _cleanup_funcs = None

    @inlineCallbacks
    def tearDown(self):
        # Run any cleanup code we've registered with .add_cleanup().
        # We do this ourselves instead of using trial's .addCleanup() because
        # that doesn't have timeouts applied to it.
        if self._cleanup_funcs is not None:
            for cleanup, args, kw in reversed(self._cleanup_funcs):
                yield cleanup(*args, **kw)

    def add_cleanup(self, func, *args, **kw):
        if self._cleanup_funcs is None:
            self._cleanup_funcs = []
        self._cleanup_funcs.append((func, args, kw))


class MessageHelper(object):
    # We can't use `None` as a placeholder for default values because we may
    # want to override the default (non-`None`) value with `None`.
    DEFAULT = object()

    def __init__(self, transport_name='sphex', transport_type='sms',
                 mobile_addr='+41791234567', transport_addr='9292'):
        self.transport_name = transport_name
        self.transport_type = transport_type
        self.mobile_addr = mobile_addr
        self.transport_addr = transport_addr

    def make_inbound(self, content, from_addr=DEFAULT, to_addr=DEFAULT, **kw):
        if from_addr is self.DEFAULT:
            from_addr = self.mobile_addr
        if to_addr is self.DEFAULT:
            to_addr = self.transport_addr
        return self.make_user_message(content, from_addr, to_addr, **kw)

    def make_outbound(self, content, from_addr=DEFAULT, to_addr=DEFAULT, **kw):
        if from_addr is self.DEFAULT:
            from_addr = self.transport_addr
        if to_addr is self.DEFAULT:
            to_addr = self.mobile_addr
        return self.make_user_message(content, from_addr, to_addr, **kw)

    def make_user_message(self, content, from_addr, to_addr, group=None,
                          session_event=None, transport_type=DEFAULT,
                          transport_name=DEFAULT, transport_metadata=DEFAULT,
                          helper_metadata=DEFAULT, **kw):
        if transport_type is self.DEFAULT:
            transport_type = self.transport_type
        if helper_metadata is self.DEFAULT:
            helper_metadata = {}
        if transport_metadata is self.DEFAULT:
            transport_metadata = {}
        if transport_name is self.DEFAULT:
            transport_name = self.transport_name
        return TransportUserMessage(
            from_addr=from_addr,
            to_addr=to_addr,
            group=group,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            helper_metadata=helper_metadata,
            content=content,
            session_event=session_event,
            **kw)

    def make_event(self, event_type, user_message_id, transport_type=DEFAULT,
                   transport_name=DEFAULT, transport_metadata=DEFAULT, **kw):
        if transport_type is self.DEFAULT:
            transport_type = self.transport_type
        if transport_name is self.DEFAULT:
            transport_name = self.transport_name
        if transport_metadata is self.DEFAULT:
            transport_metadata = {}
        return TransportEvent(
            event_type=event_type,
            user_message_id=user_message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            **kw)

    def make_ack(self, msg=None, sent_message_id=DEFAULT, **kw):
        if msg is None:
            msg = self.make_outbound("for ack")
        user_message_id = msg['message_id']
        if sent_message_id is self.DEFAULT:
            sent_message_id = user_message_id
        return self.make_event(
            'ack', user_message_id, sent_message_id=sent_message_id, **kw)

    def make_nack(self, msg=None, nack_reason=DEFAULT, **kw):
        if msg is None:
            msg = self.make_outbound("for nack")
        user_message_id = msg['message_id']
        if nack_reason is self.DEFAULT:
            nack_reason = "sunspots"
        return self.make_event(
            'nack', user_message_id, nack_reason=nack_reason, **kw)

    def make_delivery_report(self, msg=None, delivery_status=DEFAULT, **kw):
        if msg is None:
            msg = self.make_outbound("for delivery_report")
        user_message_id = msg['message_id']
        if delivery_status is self.DEFAULT:
            delivery_status = "delivered"
        return self.make_event(
            'delivery_report', user_message_id,
            delivery_status=delivery_status, **kw)
