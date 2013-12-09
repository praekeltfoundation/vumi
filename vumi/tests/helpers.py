import os
from functools import wraps

from twisted.internet.defer import succeed, inlineCallbacks, returnValue
from twisted.internet.error import ConnectionRefusedError
from twisted.python.monkey import MonkeyPatcher
from twisted.trial.unittest import TestCase, SkipTest

from zope.interface import Interface, implements

from vumi.message import TransportUserMessage, TransportEvent
from vumi.service import get_spec
from vumi.utils import vumi_resource_path, flatten_generator
from .fake_amqp import FakeAMQPBroker, FakeAMQClient


# We can't use `None` as a placeholder for default values because we may want
# to override the default (non-`None`) value with `None`.
DEFAULT = object()


class IHelper(Interface):
    def setup(*args, **kwargs):
        """Perform potentially async helper setup.

        This may return a deferred for async setup or block for sync setup. All
        helpers must implement this even if it does nothing.

        If the setup is optional but commonly used, this method can take flags
        to perform or suppress all or part of it as required.
        """

    def cleanup():
        """Clean up any resources created by this helper.

        This may return a deferred for async cleanup or block for sync cleanup.
        All helpers must implement this even if it does nothing.
        """


class IHelperEnabledTestCase(Interface):
    def add_helper(helper_object, *args, **kwargs):
        """Register cleanup and perform setup for a helper object.

        This should call `helper_object.setup(*args, **kwargs)` and
        `self.add_cleanup(helper_object.cleanup)` or an equivalent.

        Returns the `helper_object` passed in.
        """

    def add_helper_nosetup(helper_object):
        """Register cleanup for a helper object.

        This is identical to `add_helper()`, except it does not perform setup
        and is therefore guaranteed to be sync. (This is useful in cases where
        setup is not required and async operations are undesirable.)

        Returns the `helper_object` passed in.
        """


def proxyable(func):
    """Mark a method as being suitable for automatic proxy generation."""
    func.proxyable = True
    return func


def generate_proxies(target, source):
    for name in dir(source):
        attribute = getattr(source, name)
        if not getattr(attribute, 'proxyable', False):
            continue

        if hasattr(target, name):
            raise Exception(
                'Attribute already exists: %s' % (name,))
        setattr(target, name, attribute)


def get_timeout():
    # Look up the timeout in an environment variable and use a default of 5
    # seconds if there isn't one there.
    timeout_str = os.environ.get('VUMI_TEST_TIMEOUT', '5')
    return float(timeout_str)


class VumiTestCase(TestCase):
    implements(IHelperEnabledTestCase)

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

    @inlineCallbacks
    def add_helper(self, helper_object, *args, **kw):
        self.add_helper_nosetup(helper_object)
        yield helper_object.setup(*args, **kw)
        returnValue(helper_object)

    def add_helper_nosetup(self, helper_object, *args, **kw):
        if not IHelper.providedBy(helper_object):
            raise ValueError(
                "Helper object does not provide the IHelper interface: %s" % (
                    helper_object,))
        self.add_cleanup(helper_object.cleanup)
        return helper_object


class MessageHelper(object):
    implements(IHelper)

    def __init__(self, transport_name='sphex', transport_type='sms',
                 mobile_addr='+41791234567', transport_addr='9292'):
        self.transport_name = transport_name
        self.transport_type = transport_type
        self.mobile_addr = mobile_addr
        self.transport_addr = transport_addr

    def setup(self):
        pass

    def cleanup(self):
        pass

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
    implements(IHelper)

    def __init__(self, connector_name=None, broker=None):
        self._connector_name = connector_name
        self.broker = broker if broker is not None else FakeAMQPBroker()
        self._workers = []

    def setup(self):
        pass

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
    def get_fake_amqp_client(cls, broker):
        """Wrap a fake broker in an fake client.

        The broker parameter is mandatory because it's important that cleanup
        happen. If ``None`` is passed in explicitly, a new broker object will
        be created.
        """
        spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
        return FakeAMQClient(spec, {}, broker)

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
        worker._amqp_client = cls.get_fake_amqp_client(broker)
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

    implements(IHelper)

    def __init__(self, msg_helper, worker_helper):
        self.msg_helper = msg_helper
        self.worker_helper = worker_helper

    def setup(self):
        pass

    def cleanup(self):
        pass

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


class RiakDisabledForTest(object):
    """Placeholder object for a disabled riak config.

    This class exists to throw a meaningful error when trying to use Riak in
    a test that disallows it. We can't do this from inside the Riak setup
    infrastructure, because that would be very invasive for something that
    only really matters for tests.
    """
    def __getattr__(self, name):
        raise RuntimeError(
            "Use of Riak has been disabled for this test. Please set "
            "'use_riak = True' on the test class to enable it.")


def import_filter(exc, *expected):
    msg = exc.args[0]
    module = msg.split()[-1]
    if expected and (module not in expected):
        raise
    return module


def import_skip(exc, *expected):
    module = import_filter(exc, *expected)
    raise SkipTest("Failed to import '%s'." % (module,))


def maybe_async(sync_attr):
    """Decorate a method that may be sync or async.

    This redecorates with the either @inlineCallbacks or @flatten_generator,
    depending on the `sync_attr`.
    """
    if callable(sync_attr):
        # If we don't get a sync attribute name, default to 'is_sync'.
        return maybe_async('is_sync')(sync_attr)

    def redecorate(func):
        @wraps(func)
        def wrapper(self, *args, **kw):
            if getattr(self, sync_attr):
                return flatten_generator(func)(self, *args, **kw)
            return inlineCallbacks(func)(self, *args, **kw)
        return wrapper

    return redecorate


class PersistenceHelper(object):
    implements(IHelper)

    def __init__(self, use_riak=False, is_sync=False):
        self.use_riak = use_riak
        self.is_sync = is_sync
        self._patches = []
        self._riak_managers = []
        self._redis_managers = []
        self._config_overrides = {
            'redis_manager': {
                'FAKE_REDIS': 'yes',
                'key_prefix': 'vumitest',
            },
            'riak_manager': {
                'bucket_prefix': 'vumitest',
            },
        }
        if not self.use_riak:
            self._config_overrides['riak_manager'] = RiakDisabledForTest()
        self._patch_riak()
        self._patch_txriak()
        self._patch_redis()
        self._patch_txredis()

    def setup(self):
        pass

    @maybe_async
    def cleanup(self):
        for purge, manager in self._get_riak_managers_for_cleanup():
            if purge:
                try:
                    yield self._purge_riak(manager)
                except ConnectionRefusedError:
                    pass

        # Hackily close all connections left open by the non-tx riak client.
        # There's no other way to explicitly close these connections and not
        # doing it means we can hit server-side connection limits in the middle
        # of large test runs.
        for manager in self._riak_managers:
            if hasattr(manager.client, '_cm'):
                while manager.client._cm.conns:
                    manager.client._cm.conns.pop().close()

        for purge, manager in self._get_redis_managers_for_cleanup():
            if purge:
                yield self._purge_redis(manager)
            yield manager.close_manager()

        for patch in reversed(self._patches):
            patch.restore()

    def _get_riak_managers_for_cleanup(self):
        """Get a list of Riak managers and whether they should be purged.

        The return value is a list of (`bool`, `Manager`) tuples. If the first
        item is `True`, the manager should be purged. It's safe to purge
        managers even if the first item is `False`, but it adds extra cleanup
        time.
        """
        # NOTE: Assumes we're only ever connecting to one Riak cluster.
        seen_bucket_prefixes = set()
        managers = []
        for manager in self._riak_managers:
            if manager.bucket_prefix in seen_bucket_prefixes:
                managers.append((False, manager))
            else:
                seen_bucket_prefixes.add(manager.bucket_prefix)
                managers.append((True, manager))
        # Return in reverse order in case something overrides cleanup and
        # cares about ordering.
        return reversed(managers)

    def _get_redis_managers_for_cleanup(self):
        """Get a list of Redis managers and whether they should be purged.

        The return value is a list of (`bool`, `Manager`) tuples. If the first
        item is `True`, the manager should be purged. It's safe to purge
        managers even if the first item is `False`, but it adds extra cleanup
        time.
        """
        # NOTE: Assumes we're only ever connecting to one Redis db.
        seen_key_prefixes = set()
        managers = []
        for manager in self._redis_managers:
            if manager._key_prefix in seen_key_prefixes:
                managers.append((False, manager))
            else:
                seen_key_prefixes.add(manager._key_prefix)
                managers.append((True, manager))
        # Return in reverse order in case something overrides teardown and
        # cares about ordering.
        return reversed(managers)

    def _patch(self, obj, attribute, value):
        monkey_patch = MonkeyPatcher((obj, attribute, value))
        self._patches.append(monkey_patch)
        monkey_patch.patch()
        return monkey_patch

    def _patch_riak(self):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_filter(e, 'riak')
            return

        orig_init = RiakManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._riak_managers.append(obj)

        self._patch(RiakManager, '__init__', wrapper)

    def _patch_txriak(self):
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_filter(e, 'riakasaurus', 'riakasaurus.riak')
            return

        orig_init = TxRiakManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._riak_managers.append(obj)

        self._patch(TxRiakManager, '__init__', wrapper)

    def _patch_redis(self):
        try:
            from vumi.persist.redis_manager import RedisManager
        except ImportError, e:
            import_filter(e, 'redis')
            return

        orig_init = RedisManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._redis_managers.append(obj)

        self._patch(RedisManager, '__init__', wrapper)

    def _patch_txredis(self):
        from vumi.persist.txredis_manager import TxRedisManager

        orig_init = TxRedisManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._redis_managers.append(obj)

        self._patch(TxRedisManager, '__init__', wrapper)

    def _purge_riak(self, manager):
        "This is a separate method to allow easy overriding."
        return manager.purge_all()

    @maybe_async
    def _purge_redis(self, manager):
        "This is a separate method to allow easy overriding."
        try:
            yield manager._purge_all()
        except RuntimeError, e:
            # Ignore managers that are already closed.
            if e.args[0] != 'Not connected':
                raise
        yield manager.close_manager()

    @proxyable
    def get_riak_manager(self, config=None):
        if config is None:
            config = self._config_overrides['riak_manager'].copy()

        if self.is_sync:
            return self._get_sync_riak_manager(config)
        return self._get_async_riak_manager(config)

    def _get_async_riak_manager(self, config):
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_skip(e, 'riakasaurus', 'riakasaurus.riak')

        return TxRiakManager.from_config(config)

    def _get_sync_riak_manager(self, config):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')

        return RiakManager.from_config(config)

    @proxyable
    def get_redis_manager(self, config=None):
        if config is None:
            config = self._config_overrides['redis_manager'].copy()

        if self.is_sync:
            return self._get_sync_redis_manager(config)
        return self._get_async_redis_manager(config)

    def _get_async_redis_manager(self, config):
        from vumi.persist.txredis_manager import TxRedisManager

        return TxRedisManager.from_config(config)

    def _get_sync_redis_manager(self, config):
        try:
            from vumi.persist.redis_manager import RedisManager
        except ImportError, e:
            import_skip(e, 'redis')

        return RedisManager.from_config(config)

    @proxyable
    def mk_config(self, config):
        config = config.copy()
        config.update(self._config_overrides)
        return config
