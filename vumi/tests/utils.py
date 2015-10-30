# -*- test-case-name: vumi.tests.test_testutils -*-

import re
from datetime import datetime, timedelta
import warnings
from functools import wraps

import pytz
from twisted.internet import reactor
from twisted.internet.error import ConnectionRefusedError
from twisted.web.resource import Resource
from twisted.internet.defer import DeferredQueue, inlineCallbacks
from twisted.python import log
from twisted.python.monkey import MonkeyPatcher

from vumi.utils import flatten_generator, LogFilterSite

# For backcompat:
from vumi.tests.helpers import import_filter, import_skip
import_filter, import_skip  # To keep pyflakes happy.


class UTCNearNow(object):
    def __init__(self, offset=10):
        self.now = datetime.utcnow()
        self.utcnow = self.now.replace(tzinfo=pytz.UTC)
        self.offset = timedelta(offset)

    def __eq__(self, other):
        now = self.now
        if other.tzinfo:
            now = self.utcnow
        return (now - self.offset) < other < (now + self.offset)


class RegexMatcher(object):
    def __init__(self, regex):
        self.regex = re.compile(regex)

    def __eq__(self, other):
        return self.regex.match(other)


def FakeRedis():
    warnings.warn("Use of FakeRedis is deprecated. "
                  "Use persist.tests.fake_redis instead.",
                  category=DeprecationWarning)
    from vumi.persist import fake_redis
    return fake_redis.FakeRedis()


class LogCatcher(object):
    """Context manager for gathering logs in tests.

    :param str system:
        Only log events whose 'system' value contains the given
        regular expression pattern will be gathered. Default: None
        (i.e. keep all log events).

    :param str message:
        Only log events whose message contains the given regular
        expression pattern will be gathered. The message is
        constructed by joining the elements in the 'message' value
        with a space (the same way Twisted does). Default: None
        (i.e. keep all log events).

    :param int log_level:
        Only log events whose logLevel is equal to the given level
        will be gathered. Default: None (i.e. keep all log events).
    """

    def __init__(self, system=None, message=None, log_level=None):
        self.logs = []
        self.system = re.compile(system) if system is not None else None
        self.message = re.compile(message) if message is not None else None
        self.log_level = log_level

    @property
    def errors(self):
        return [ev for ev in self.logs if ev["isError"]]

    def messages(self):
        return [" ".join(msg['message']) for msg in self.logs
                if not msg["isError"]]

    def _keep_log(self, event_dict):
        if self.system is not None:
            if not self.system.search(event_dict.get('system', '-')):
                return False
        if self.message is not None:
            log_message = " ".join(event_dict.get('message', []))
            if not self.message.search(log_message):
                return False
        if self.log_level is not None:
            if event_dict.get('logLevel', None) != self.log_level:
                return False
        return True

    def _gather_logs(self, event_dict):
        if self._keep_log(event_dict):
            self.logs.append(event_dict)

    def __enter__(self):
        log.theLogPublisher.addObserver(self._gather_logs)
        return self

    def __exit__(self, *exc_info):
        log.theLogPublisher.removeObserver(self._gather_logs)


class MockResource(Resource):
    isLeaf = True

    def __init__(self, handler):
        Resource.__init__(self)
        self.handler = handler

    def render_GET(self, request):
        return self.handler(request)

    def render_POST(self, request):
        return self.handler(request)

    def render_PUT(self, request):
        return self.handler(request)


class MockHttpServer(object):
    """
    NOTE: This is deprecated.
          Please use :class:`vumi.tests.http_helpers.MockHttpHelper` instead.
    """

    def __init__(self, handler=None):
        self.queue = DeferredQueue()
        self._handler = handler or self.handle_request
        self._webserver = None
        self.addr = None
        self.url = None

    def handle_request(self, request):
        self.queue.put(request)

    @inlineCallbacks
    def start(self):
        root = MockResource(self._handler)
        site_factory = LogFilterSite(root)
        self._webserver = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        self.addr = self._webserver.getHost()
        self.url = "http://%s:%s/" % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def stop(self):
        yield self._webserver.stopListening()
        yield self._webserver.loseConnection()


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


class PersistenceMixin(object):
    sync_persistence = False
    use_riak = False

    sync_or_async = staticmethod(maybe_async('sync_persistence'))

    def _persist_setUp(self):
        warnings.warn("PersistenceMixin is deprecated. "
                      "Use PersistenceHelper from vumi.tests.helpers instead.",
                      category=DeprecationWarning)
        self._persist_patches = []
        self._persist_riak_managers = []
        self._persist_redis_managers = []
        self._persist_config = {
            'redis_manager': {
                'FAKE_REDIS': 'yes',
                'key_prefix': type(self).__module__,
                },
            'riak_manager': {
                'bucket_prefix': type(self).__module__,
                },
            }
        if not self.use_riak:
            self._persist_config['riak_manager'] = RiakDisabledForTest()
        self._persist_patch_riak()
        self._persist_patch_txriak()
        self._persist_patch_redis()
        self._persist_patch_txredis()

    def mk_config(self, config):
        return dict(self._persist_config, **config)

    @maybe_async('sync_persistence')
    def _persist_tearDown(self):
        for purge, manager in self._persist_get_teardown_riak_managers():
            if purge:
                try:
                    yield self._persist_purge_riak(manager)
                except ConnectionRefusedError:
                    pass

        # Hackily close all connections left open by the non-tx riak client.
        # There's no other way to explicitly close these connections and not
        # doing it means we can hit server-side connection limits in the middle
        # of large test runs.
        for manager in self._persist_riak_managers:
            if hasattr(manager.client, '_cm'):
                while manager.client._cm.conns:
                    manager.client._cm.conns.pop().close()

        for purge, manager in self._persist_get_teardown_redis_managers():
            if purge:
                yield self._persist_purge_redis(manager)
            yield manager.close_manager()

        for patch in reversed(self._persist_patches):
            patch.restore()

    def _persist_get_teardown_riak_managers(self):
        """Get a list of Riak managers and whether they should be purged.

        The return value is a list of (`bool`, `Manager`) tuples. If the first
        item is `True`, the manager should be purged. It's safe to purge
        managers even if the first item is `False`, but it adds extra cleanup
        time.
        """
        # NOTE: Assumes we're only ever connecting to one Riak cluster.
        seen_bucket_prefixes = set()
        managers = []
        for manager in self._persist_riak_managers:
            if manager.bucket_prefix in seen_bucket_prefixes:
                managers.append((False, manager))
            else:
                seen_bucket_prefixes.add(manager.bucket_prefix)
                managers.append((True, manager))
        # Return in reverse order in case something overrides teardown and
        # cares about ordering.
        return reversed(managers)

    def _persist_get_teardown_redis_managers(self):
        """Get a list of Redis managers and whether they should be purged.

        The return value is a list of (`bool`, `Manager`) tuples. If the first
        item is `True`, the manager should be purged. It's safe to purge
        managers even if the first item is `False`, but it adds extra cleanup
        time.
        """
        # NOTE: Assumes we're only ever connecting to one Redis db.
        seen_key_prefixes = set()
        managers = []
        for manager in self._persist_redis_managers:
            if manager._key_prefix in seen_key_prefixes:
                managers.append((False, manager))
            else:
                seen_key_prefixes.add(manager._key_prefix)
                managers.append((True, manager))
        # Return in reverse order in case something overrides teardown and
        # cares about ordering.
        return reversed(managers)

    def _persist_patch(self, obj, attribute, value):
        monkey_patch = MonkeyPatcher((obj, attribute, value))
        self._persist_patches.append(monkey_patch)
        monkey_patch.patch()
        return monkey_patch

    def _persist_patch_riak(self):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_filter(e, 'riak')
            return

        orig_init = RiakManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._persist_riak_managers.append(obj)

        self._persist_patch(RiakManager, '__init__', wrapper)

    def _persist_patch_txriak(self):
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_filter(e, 'riak')
            return

        orig_init = TxRiakManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._persist_riak_managers.append(obj)

        self._persist_patch(TxRiakManager, '__init__', wrapper)

    def _persist_patch_redis(self):
        try:
            from vumi.persist.redis_manager import RedisManager
        except ImportError, e:
            import_filter(e, 'redis')
            return

        orig_init = RedisManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._persist_redis_managers.append(obj)

        self._persist_patch(RedisManager, '__init__', wrapper)

    def _persist_patch_txredis(self):
        from vumi.persist.txredis_manager import TxRedisManager

        orig_init = TxRedisManager.__init__

        def wrapper(obj, *args, **kw):
            orig_init(obj, *args, **kw)
            self._persist_redis_managers.append(obj)

        self._persist_patch(TxRedisManager, '__init__', wrapper)

    def _persist_purge_riak(self, manager):
        "This is a separate method to allow easy overriding."
        return manager.purge_all()

    @maybe_async('sync_persistence')
    def _persist_purge_redis(self, manager):
        "This is a separate method to allow easy overriding."
        try:
            yield manager._purge_all()
        except RuntimeError, e:
            # Ignore managers that are already closed.
            if e.args[0] != 'Not connected':
                raise
        yield manager.close_manager()

    def get_riak_manager(self, config=None):
        if config is None:
            config = self._persist_config['riak_manager'].copy()

        if self.sync_persistence:
            return self._get_sync_riak_manager(config)
        return self._get_async_riak_manager(config)

    def _get_async_riak_manager(self, config):
        try:
            from vumi.persist.txriak_manager import TxRiakManager
        except ImportError, e:
            import_skip(e, 'riak')

        return TxRiakManager.from_config(config)

    def _get_sync_riak_manager(self, config):
        try:
            from vumi.persist.riak_manager import RiakManager
        except ImportError, e:
            import_skip(e, 'riak')

        return RiakManager.from_config(config)

    def get_redis_manager(self, config=None):
        if config is None:
            config = self._persist_config['redis_manager'].copy()

        if self.sync_persistence:
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
