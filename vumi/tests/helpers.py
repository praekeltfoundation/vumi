import os

from twisted.internet.defer import succeed, inlineCallbacks
from twisted.trial.unittest import TestCase

from vumi.message import TransportUserMessage, TransportEvent
from vumi.service import get_spec
from vumi.utils import vumi_resource_path
from .fake_amqp import FakeAMQPBroker, FakeAMQClient


# We can't use `None` as a placeholder for default values because we may want
# to override the default (non-`None`) value with `None`.
DEFAULT = object()


def proxyable(func):
    """Mark a method as being suitable for automatic proxy generation."""
    func.proxyable = True
    return func


def generate_proxies(target, source, suffix=None):
    for name in dir(source):
        attribute = getattr(source, name)
        if not getattr(attribute, 'proxyable', False):
            continue

        target_name = name
        if suffix is not None:
            target_name = '_'.join([target_name, suffix])

        if hasattr(target, target_name):
            raise Exception(
                'Attribute already exists: %s' % (target_name,))
        setattr(target, target_name, attribute)


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

    def __init__(self, transport_name='sphex', transport_type='sms',
                 mobile_addr='+41791234567', transport_addr='9292'):
        self.transport_name = transport_name
        self.transport_type = transport_type
        self.mobile_addr = mobile_addr
        self.transport_addr = transport_addr

    @proxyable
    def make_inbound(self, content, from_addr=DEFAULT, to_addr=DEFAULT, **kw):
        if from_addr is DEFAULT:
            from_addr = self.mobile_addr
        if to_addr is DEFAULT:
            to_addr = self.transport_addr
        return self.make_user_message(content, from_addr, to_addr, **kw)

    @proxyable
    def make_outbound(self, content, from_addr=DEFAULT, to_addr=DEFAULT, **kw):
        if from_addr is DEFAULT:
            from_addr = self.transport_addr
        if to_addr is DEFAULT:
            to_addr = self.mobile_addr
        return self.make_user_message(content, from_addr, to_addr, **kw)

    @proxyable
    def make_user_message(self, content, from_addr, to_addr, group=None,
                          session_event=None, transport_type=DEFAULT,
                          transport_name=DEFAULT, transport_metadata=DEFAULT,
                          helper_metadata=DEFAULT, endpoint=DEFAULT, **kw):
        if transport_type is DEFAULT:
            transport_type = self.transport_type
        if helper_metadata is DEFAULT:
            helper_metadata = {}
        if transport_metadata is DEFAULT:
            transport_metadata = {}
        if transport_name is DEFAULT:
            transport_name = self.transport_name
        msg = TransportUserMessage(
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
        if endpoint is not DEFAULT:
            msg.set_routing_endpoint(endpoint)
        return msg

    @proxyable
    def make_event(self, event_type, user_message_id, transport_type=DEFAULT,
                   transport_name=DEFAULT, transport_metadata=DEFAULT,
                   endpoint=DEFAULT, **kw):
        if transport_type is DEFAULT:
            transport_type = self.transport_type
        if transport_name is DEFAULT:
            transport_name = self.transport_name
        if transport_metadata is DEFAULT:
            transport_metadata = {}
        msg = TransportEvent(
            event_type=event_type,
            user_message_id=user_message_id,
            transport_name=transport_name,
            transport_type=transport_type,
            transport_metadata=transport_metadata,
            **kw)
        if endpoint is not DEFAULT:
            msg.set_routing_endpoint(endpoint)
        return msg

    @proxyable
    def make_ack(self, msg=None, sent_message_id=DEFAULT, **kw):
        if msg is None:
            msg = self.make_outbound("for ack")
        user_message_id = msg['message_id']
        if sent_message_id is DEFAULT:
            sent_message_id = user_message_id
        return self.make_event(
            'ack', user_message_id, sent_message_id=sent_message_id, **kw)

    @proxyable
    def make_nack(self, msg=None, nack_reason=DEFAULT, **kw):
        if msg is None:
            msg = self.make_outbound("for nack")
        user_message_id = msg['message_id']
        if nack_reason is DEFAULT:
            nack_reason = "sunspots"
        return self.make_event(
            'nack', user_message_id, nack_reason=nack_reason, **kw)

    @proxyable
    def make_delivery_report(self, msg=None, delivery_status=DEFAULT, **kw):
        if msg is None:
            msg = self.make_outbound("for delivery_report")
        user_message_id = msg['message_id']
        if delivery_status is DEFAULT:
            delivery_status = "delivered"
        return self.make_event(
            'delivery_report', user_message_id,
            delivery_status=delivery_status, **kw)

    @proxyable
    def make_reply(self, msg, content, **kw):
        return msg.reply(content, **kw)


def _start_and_return_worker(worker):
    return worker.startWorker().addCallback(lambda r: worker)


class WorkerHelper(object):
    def __init__(self, connector_name=None, broker=None):
        self._connector_name = connector_name
        self.broker = broker if broker is not None else FakeAMQPBroker()
        self._workers = []

    @inlineCallbacks
    def cleanup(self):
        # Wait for any pending message deliveries to avoid a race with a dirty
        # reactor.
        yield self.broker.wait_delivery()
        # Now stop all the workers we created.
        for worker in self._workers:
            yield worker.stopWorker()

    @proxyable
    def cleanup_worker(self, worker):
        """Clean up a particular worker manually."""
        self._workers.remove(worker)
        return worker.stopWorker()

    @classmethod
    def get_worker_raw(cls, worker_class, config, broker=None):
        """Create and return an instance of a vumi worker.

        This doesn't start the worker and it doesn't add it to any cleanup
        machinery. In most cases, you want :meth:`get_worker` instead.
        """

        # When possible, always try and enable heartbeat setup in tests.
        # so make sure worker_name is set
        if (config is not None) and ('worker_name' not in config):
            config['worker_name'] = "unnamed"

        worker = worker_class({}, config)
        spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
        worker._amqp_client = FakeAMQClient(spec, {}, broker)
        return worker

    @proxyable
    def get_worker(self, worker_class, config, start=True):
        """Create and return an instance of a vumi worker.

        :param worker_class: The worker class to instantiate.
        :param config: Config dict.
        :param start: True to start the worker (default), False otherwise.
        """
        worker = self.get_worker_raw(worker_class, config, self.broker)

        self._workers.append(worker)
        d = succeed(worker)
        if start:
            d.addCallback(_start_and_return_worker)
        return d

    def _rkey(self, connector_name, name):
        if connector_name is None:
            connector_name = self._connector_name
        return '.'.join((connector_name, name))

    @proxyable
    def get_dispatched(self, connector_name, name, message_class):
        msgs = self.broker.get_dispatched(
            'vumi', self._rkey(connector_name, name))
        return [message_class.from_json(msg.body) for msg in msgs]

    def _wait_for_dispatched(self, connector_name, name, amount):
        rkey = self._rkey(connector_name, name)
        return self.broker.wait_messages('vumi', rkey, amount)

    @proxyable
    def clear_all_dispatched(self):
        self.broker.clear_messages('vumi')

    def _clear_dispatched(self, connector_name, name):
        rkey = self._rkey(connector_name, name)
        return self.broker.clear_messages('vumi', rkey)

    @proxyable
    def get_dispatched_events(self, connector_name=None):
        return self.get_dispatched(connector_name, 'event', TransportEvent)

    @proxyable
    def get_dispatched_inbound(self, connector_name=None):
        return self.get_dispatched(
            connector_name, 'inbound', TransportUserMessage)

    @proxyable
    def get_dispatched_outbound(self, connector_name=None):
        return self.get_dispatched(
            connector_name, 'outbound', TransportUserMessage)

    @proxyable
    def wait_for_dispatched_events(self, amount, connector_name=None):
        d = self._wait_for_dispatched(connector_name, 'event', amount)
        d.addCallback(lambda msgs: [
            TransportEvent(**msg.payload) for msg in msgs])
        return d

    @proxyable
    def wait_for_dispatched_inbound(self, amount, connector_name=None):
        d = self._wait_for_dispatched(connector_name, 'inbound', amount)
        d.addCallback(lambda msgs: [
            TransportUserMessage(**msg.payload) for msg in msgs])
        return d

    @proxyable
    def wait_for_dispatched_outbound(self, amount, connector_name=None):
        d = self._wait_for_dispatched(connector_name, 'outbound', amount)
        d.addCallback(lambda msgs: [
            TransportUserMessage(**msg.payload) for msg in msgs])
        return d

    @proxyable
    def clear_dispatched_events(self, connector_name=None):
        return self._clear_dispatched(connector_name, 'event')

    @proxyable
    def clear_dispatched_inbound(self, connector_name=None):
        return self._clear_dispatched(connector_name, 'inbound')

    @proxyable
    def clear_dispatched_outbound(self, connector_name=None):
        return self._clear_dispatched(connector_name, 'outbound')

    @proxyable
    def dispatch_raw(self, routing_key, message, exchange='vumi'):
        self.broker.publish_message(exchange, routing_key, message)
        return self.kick_delivery()

    @proxyable
    def dispatch_inbound(self, message, connector_name=None):
        return self.dispatch_raw(
            self._rkey(connector_name, 'inbound'), message)

    @proxyable
    def dispatch_outbound(self, message, connector_name=None):
        return self.dispatch_raw(
            self._rkey(connector_name, 'outbound'), message)

    @proxyable
    def dispatch_event(self, message, connector_name=None):
        return self.dispatch_raw(
            self._rkey(connector_name, 'event'), message)

    @proxyable
    def kick_delivery(self):
        return self.broker.kick_delivery()


class MessageDispatchHelper(object):
    """Helper for creating and immediately dispatching messages.

    This builds on top of :class:`MessageHelper` and :class:`WorkerHelper`.

    It does not allow dispatching to nonstandard connectors. If you need to do
    that, either use :class:`MessageHelper` and :class:`WorkerHelper` directly
    or build a second :class:`MessageDispatchHelper` with a second
    :class:`WorkerHelper`.
    """
    def __init__(self, msg_helper, worker_helper):
        self.msg_helper = msg_helper
        self.worker_helper = worker_helper

    @proxyable
    def make_dispatch_inbound(self, *args, **kw):
        msg = self.msg_helper.make_inbound(*args, **kw)
        d = self.worker_helper.dispatch_inbound(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_outbound(self, *args, **kw):
        msg = self.msg_helper.make_outbound(*args, **kw)
        d = self.worker_helper.dispatch_outbound(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_ack(self, *args, **kw):
        msg = self.msg_helper.make_ack(*args, **kw)
        d = self.worker_helper.dispatch_event(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_nack(self, *args, **kw):
        msg = self.msg_helper.make_nack(*args, **kw)
        d = self.worker_helper.dispatch_event(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_delivery_report(self, *args, **kw):
        msg = self.msg_helper.make_delivery_report(*args, **kw)
        d = self.worker_helper.dispatch_event(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_reply(self, *args, **kw):
        msg = self.msg_helper.make_reply(*args, **kw)
        d = self.worker_helper.dispatch_outbound(msg)
        return d.addCallback(lambda r: msg)
