import json
import os
from functools import wraps
from inspect import CO_GENERATOR

from twisted.internet.defer import succeed, inlineCallbacks, Deferred
from twisted.internet.error import ConnectionRefusedError
from twisted.internet.task import deferLater
from twisted.python.failure import Failure
from twisted.python.monkey import MonkeyPatcher
from twisted.trial.unittest import TestCase, SkipTest, FailTest

from zope.interface import Interface, implements

from vumi.message import TransportUserMessage, TransportEvent
from vumi.service import get_spec
from vumi.utils import vumi_resource_path, flatten_generator
from vumi.tests.fake_amqp import FakeAMQPBroker, FakeAMQClient


class _Default(object):
    """
    Placeholder for default values in helpers.

    We can't use ``None`` as a placeholder for default values because we may
    want to override the default (non-``None``) value with ``None``.

    This is its own class (rather than using an instance of ``object``) because
    Sphinx uses ``repr()`` on function parameter defaults when generating API
    documentation and we'd rather see ``DEFAULT`` in the docs than ``<object
    object at 0x1010552c0>``.
    """

    def __repr__(self):
        return 'DEFAULT'

DEFAULT = _Default()


class IHelper(Interface):
    """
    Interface for test helpers.

    This specifies a standard setup and cleanup mechanism used by test cases
    that implement the :class:`IHelperEnabledTestCase` interface.

    There are no interface restrictions on the constructor of a helper.
    """

    def setup(*args, **kwargs):
        """
        Perform potentially async helper setup.

        This may return a deferred for async setup or block for sync setup. All
        helpers must implement this even if it does nothing.

        If the setup is optional but commonly used, this method can take flags
        to perform or suppress all or part of it as required.
        """

    def cleanup():
        """
        Clean up any resources created by this helper.

        This may return a deferred for async cleanup or block for sync cleanup.
        All helpers must implement this even if it does nothing.
        """


class IHelperEnabledTestCase(Interface):
    """
    Interface for test cases that use helpers.

    This specifies a standard mechanism for managing setup and cleanup of
    helper classes that implement the :class:`IHelper` interface.
    """

    def add_helper(helper_object, *args, **kwargs):
        """
        Register cleanup and perform setup for a helper object.

        This should call ``helper_object.setup(*args, **kwargs)`` and
        ``self.add_cleanup(helper_object.cleanup)`` or an equivalent.

        Returns the ``helper_object`` passed in or a :class:`Deferred` if
        setup is async.
        """


def proxyable(func):
    """
    Mark a method as being suitable for automatic proxy generation.

    See :func:`generate_proxies` for usage.
    """
    func.proxyable = True
    return func


def generate_proxies(target, source):
    """
    Generate proxies on ``target`` for proxyable methods on ``source``.

    This is useful for wrapping helper objects in higher-level helpers or
    extending a helper to provide extra functionality without having to resort
    to subclassing.

    The "proxying" is actually just copying the proxyable attribute onto the
    target.

    >>> class AddHelper(object):
    ...     def __init__(self, number):
    ...         self._number = number
    ...
    ...     @proxyable
    ...     def add_number(self, number):
    ...         return self._number + number

    >>> class OtherHelper(object):
    ...     def __init__(self, number):
    ...         self._adder = AddHelper(number)
    ...         generate_proxies(self, self._adder)
    ...
    ...     @proxyable
    ...     def say_hello(self):
    ...         return "hello"

    >>> other_helper = OtherHelper(3)
    >>> other_helper.say_hello()
    'hello'
    >>> other_helper.add_number(2)
    5
    """

    for name in dir(source):
        attribute = getattr(source, name)
        if not getattr(attribute, 'proxyable', False):
            continue

        if hasattr(target, name):
            raise Exception(
                'Attribute already exists: %s' % (name,))
        setattr(target, name, attribute)


def success_result_of(d):
    """
    We can't necessarily use TestCase.successResultOf because our Twisted might
    not be new enough. This is a standalone copy with some minor message
    differences.
    """
    results = []
    d.addBoth(results.append)
    if not results:
        raise FailTest("No result available for deferred: %r" % (d,))
    if isinstance(results[0], Failure):
        raise FailTest("Expected success from deferred %r, got failure: %r" % (
            d, results[0]))
    return results[0]


def get_timeout():
    """
    Look up the test timeout in the ``VUMI_TEST_TIMEOUT`` environment variable.

    A default of 5 seconds is used if there isn't one there.
    """
    timeout_str = os.environ.get('VUMI_TEST_TIMEOUT', '5')
    return float(timeout_str)


class VumiTestCase(TestCase):
    """
    Base test case class for all things vumi-related.

    This is a subclass of :class:`twisted.trial.unittest.TestCase` with a small
    number of additional features:

    * It implements :class:`IHelperEnabledTestCase` to make using helpers
      easier. (See :meth:`add_helper`.)

    * :attr:`timeout` is set to a default value of ``5`` and can be overridden
      by setting the ``VUMI_TEST_TIMEOUT`` environment variable. (Longer
      timeouts are more reliable for continuous integration builds, shorter
      ones are less painful for local development.)

    * :meth:`add_cleanup` provides an alternative mechanism for specifying
      cleanup in the same place as the creation of thing that needs to be
      cleaned up.

    .. note::

       While this class does not have a :meth:`setUp` method (thus avoiding the
       need for subclasses to call it), it *does* have a :meth:`tearDown`
       method. :meth:`add_cleanup` should be used in subclasses instead of
       overriding :meth:`tearDown`.
    """

    implements(IHelperEnabledTestCase)

    timeout = get_timeout()
    reactor_check_interval = 0.01  # 10ms, no science behind this number.
    reactor_check_iterations = 100  # No science behind this number either.

    _cleanup_funcs = None

    @inlineCallbacks
    def tearDown(self):
        """
        Run any cleanup functions registered with :meth:`add_cleanup`.
        """
        # Run any cleanup code we've registered with .add_cleanup().
        # We do this ourselves instead of using trial's .addCleanup() because
        # that doesn't have timeouts applied to it.
        if self._cleanup_funcs is not None:
            for cleanup, args, kw in reversed(self._cleanup_funcs):
                yield cleanup(*args, **kw)
        yield self._check_reactor_things()

    @inlineCallbacks
    def _check_reactor_things(self):
        """
        Poll the reactor for unclosed connections and wait for them to close.

        Properly waiting for all connections to finish closing requires hooking
        into :meth:`Protocol.connectionLost` in both client and server. Since
        this isn't practical in all cases, we check the reactor for any open
        connections and wait a bit for them to finish closing if we find any.

        NOTE: This will only wait for connections that close on their own. Any
              connections that have been left open will stay open (unless they
              time out or something) and will leave the reactor dirty after we
              stop waiting.
        """
        from twisted.internet import reactor
        # Give the reactor a chance to get clean.
        yield deferLater(reactor, 0, lambda: None)

        for i in range(self.reactor_check_iterations):
            # There are some internal readers that we want to ignore.
            # Unfortunately they're private.
            internal_readers = getattr(reactor, '_internalReaders', set())
            selectables = set(reactor.getReaders() + reactor.getWriters())
            if not (selectables - internal_readers):
                # The reactor's clean, let's go home.
                return

            # We haven't gone home, so wait a bit for selectables to go away.
            yield deferLater(
                reactor, self.reactor_check_interval, lambda: None)

    def add_cleanup(self, func, *args, **kw):
        """
        Register a cleanup function to be called at teardown time.

        :param callable func:
            The callable object to call at cleanup time. This callable may
            return a :class:`Deferred`, in which case cleanup will continue
            after it fires.
        :param \*args: Passed to ``func`` when it is called.
        :param \**kw: Passed to ``func`` when it is called.

        .. note::
           This method should be use in place of the inherited
           :meth:`addCleanup` method, because the latter doesn't apply timeouts
           to cleanup functions.
        """
        if self._cleanup_funcs is None:
            self._cleanup_funcs = []
        self._cleanup_funcs.append((func, args, kw))

    def add_helper(self, helper_object, *args, **kw):
        """
        Perform setup and register cleanup for the given helper object.

        :param helper_object:
            Helper object to add. ``helper_object`` must provide the
            :class:`IHelper` interface.
        :param \*args: Passed to :meth:`helper_object.setup` when it is called.
        :param \**kw: Passed to :meth:`helper_object.setup` when it is called.

        :returns:
            Either ``helper_object`` or a :class:`Deferred` that fires with it.

        If :meth:`helper_object.setup` returns a :class:`Deferred`, this method
        also returns a :class:`Deferred`.

        Example usage assuming ``@inlineCallbacks``:

        >>> @inlineCallbacks
        ... def test_foo(self):
        ...     msg_helper = yield self.add_helper(MessageHelper())
        ...     msg_helper.make_inbound("foo")

        Example usage assuming non-async setup:

        >>> def test_bar(self):
        ...     msg_helper = self.add_helper(MessageHelper())
        ...     msg_helper.make_inbound("bar")

        """

        if not IHelper.providedBy(helper_object):
            raise ValueError(
                "Helper object does not provide the IHelper interface: %s" % (
                    helper_object,))
        self.add_cleanup(helper_object.cleanup)
        return maybe_async_return(
            helper_object, helper_object.setup(*args, **kw))

    def _runFixturesAndTest(self, result):
        """
        Override trial's ``_runFixturesAndTest()`` method to detect test
        methods that are generator functions, indicating a missing
        ``@inlineCallbacks`` decorator.

        NOTE: This should probably be removed when
              https://twistedmatrix.com/trac/ticket/3917 is merged and the next
              Twisted version (probably 14.0) is released.
        """
        method = getattr(self, self._testMethodName)
        if method.func_code.co_flags & CO_GENERATOR:
            # We have a generator that isn't wrapped in @inlineCallbacks
            e = ValueError(
                "Test method is a generator. Missing @inlineCallbacks?")
            result.addError(self, Failure(e))
            return
        return super(VumiTestCase, self)._runFixturesAndTest(result)


class MessageHelper(object):
    """
    Test helper for constructing various messages.

    This helper does no setup or cleanup. It takes the following parameters,
    which are used as defaults for message fields:

    :param str transport_name:
        Default value for ``transport_name`` on all messages.

    :param str transport_type:
        Default value for ``transport_type`` on all messages.

    :param str mobile_addr:
        Default value for ``from_addr`` on inbound messages and ``to_addr`` on
        outbound messages.

    :param str transport_addr:
        Default value for ``to_addr`` on inbound messages and ``from_addr`` on
        outbound messages.
    """

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
        """
        Construct an inbound :class:`~vumi.message.TransportUserMessage`.

        This is a convenience wrapper around :meth:`make_user_message` and just
        sets ``to_addr`` and ``from_addr`` appropriately for an inbound
        message.
        """
        if from_addr is DEFAULT:
            from_addr = self.mobile_addr
        if to_addr is DEFAULT:
            to_addr = self.transport_addr
        return self.make_user_message(content, from_addr, to_addr, **kw)

    @proxyable
    def make_outbound(self, content, from_addr=DEFAULT, to_addr=DEFAULT, **kw):
        """
        Construct an outbound :class:`~vumi.message.TransportUserMessage`.

        This is a convenience wrapper around :meth:`make_user_message` and just
        sets ``to_addr`` and ``from_addr`` appropriately for an outbound
        message.
        """
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
        """
        Construct a :class:`~vumi.message.TransportUserMessage`.

        This method is the underlying implementation for :meth:`make_inbound`
        and :meth:`make_outbound` and those should be used instead where they
        apply.

        The only real difference between using this method and constructing a
        message object directly is that this method provides sensible defaults
        for most fields and sets the routing endpoint (if provided) in a more
        convenient way.

        The following parameters are mandatory:

        :param str content: Message ``content`` field.
        :param str from_addr: Message ``from_addr`` field.
        :param str to_addr: Message ``to_addr`` field.

        The following parameters override default values for the message fields
        of the same name:

        :param str group: Default ``None``.
        :param str session_event: Default ``None``.
        :param str transport_type: Default :attr:`transport_type`.
        :param str transport_name: Default :attr:`transport_name`.
        :param dict transport_metadata: Default ``{}``.
        :param dict helper_metadata: Default ``{}``.

        The following parameter is special:

        :param str endpoint:
            If specified, the routing endpoint on the message is set by calling
            :meth:`TransportUserMessage.set_routing_endpoint`.

        All other keyword args are passed to the
        :class:`~vumi.message.TransportUserMessage` constructor.
        """
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
        """
        Construct a :class:`~vumi.message.TransportEvent`.

        This method is the underlying implementation for :meth:`make_ack`,
        :meth:`make_nack` and :meth:`make_delivery_report`. Those should
        be used instead where they apply.

        The only real difference between using this method and constructing an
        event object directly is that this method provides sensible defaults
        for most fields and sets the routing endpoint (if provided) in a more
        convenient way.

        The following parameters are mandatory:

        :param str event_type: Event ``event_type`` field.
        :param str user_message_id: Event ``user_message_id`` field.

        Any fields required by a particular event type (such as
        ``sent_message_id`` for ``ack`` events) are also mandatory.

        The following parameters override default values for the event fields
        of the same name:

        :param str transport_type: Default :attr:`transport_type`.
        :param str transport_name: Default :attr:`transport_name`.
        :param dict transport_metadata: Default ``{}``.

        The following parameter is special:

        :param str endpoint:
            If specified, the routing endpoint on the event is set by calling
            :meth:`TransportUserMessage.set_routing_endpoint`.

        All other keyword args are passed to the
        :class:`~vumi.message.TransportEvent` constructor.
        """
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
        """
        Construct an 'ack' :class:`~vumi.message.TransportEvent`.

        :param msg:
            :class:`~vumi.message.TransportUserMessage` instance the event is
            for. If ``None``, this method will call :meth:`make_outbound` to
            get one.
        :param str sent_message_id:
            If this isn't provided, ``msg['message_id']`` will be used.

        All remaining keyword params are passed to :meth:`make_event`.
        """
        if msg is None:
            msg = self.make_outbound("for ack")
        user_message_id = msg['message_id']
        if sent_message_id is DEFAULT:
            sent_message_id = user_message_id
        return self.make_event(
            'ack', user_message_id, sent_message_id=sent_message_id, **kw)

    @proxyable
    def make_nack(self, msg=None, nack_reason=DEFAULT, **kw):
        """
        Construct a 'nack' :class:`~vumi.message.TransportEvent`.

        :param msg:
            :class:`~vumi.message.TransportUserMessage` instance the event is
            for. If ``None``, this method will call :meth:`make_outbound` to
            get one.
        :param str nack_reason:
            If this isn't provided, a suitable excuse will be used.

        All remaining keyword params are passed to :meth:`make_event`.
        """
        if msg is None:
            msg = self.make_outbound("for nack")
        user_message_id = msg['message_id']
        if nack_reason is DEFAULT:
            nack_reason = "sunspots"
        return self.make_event(
            'nack', user_message_id, nack_reason=nack_reason, **kw)

    @proxyable
    def make_delivery_report(self, msg=None, delivery_status=DEFAULT, **kw):
        """
        Construct a 'delivery_report' :class:`~vumi.message.TransportEvent`.

        :param msg:
            :class:`~vumi.message.TransportUserMessage` instance the event is
            for. If ``None``, this method will call :meth:`make_outbound` to
            get one.
        :param str delivery_status:
            If this isn't provided, ``"delivered"`` will be used.

        All remaining keyword params are passed to :meth:`make_event`.
        """
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
        """
        Construct a reply :class:`~vumi.message.TransportUserMessage`.

        This literally just calls ``msg.reply(content, **kw)``. It is included
        for completeness and symmetry with
        :meth:`MessageDispatchHelper.make_dispatch_reply`.
        """
        return msg.reply(content, **kw)


def _start_and_return_worker(worker):
    return worker.startWorker().addCallback(lambda r: worker)


class WorkerHelper(object):
    """
    Test helper for creating workers and dispatching messages.

    This helper does no setup, but it waits for pending message deliveries and
    the stops all workers it knows about during cleanup. It takes the following
    parameters:

    :param str connector_name:
        Default value for ``connector_name`` on all message broker operations.
        If ``None``, the connector name must be provided for each operation.

    :param broker:
        The message broker to use internally. This should be an instance of
        :class:`~vumi.tests.fake_amqp.FakeAMQPBroker` if it is provided, but
        most of the time the default of ``None`` should be used to have the
        helper create its own broker.
    """

    implements(IHelper)

    def __init__(self, connector_name=None, broker=None):
        self._connector_name = connector_name
        self.broker = broker if broker is not None else FakeAMQPBroker()
        self._workers = []

    def setup(self):
        pass

    @inlineCallbacks
    def cleanup(self):
        """
        Wait for any pending message deliveries and stop all workers.
        """
        yield self.broker.wait_delivery()
        for worker in self._workers:
            yield worker.stopWorker()

    @proxyable
    def cleanup_worker(self, worker):
        """
        Clean up a particular worker manually and remove it from the helper's
        cleanup list. This should only be called with workers that are already
        in the helper's cleanup list.
        """
        self._workers.remove(worker)
        return worker.stopWorker()

    @classmethod
    def get_fake_amqp_client(cls, broker):
        """
        Wrap a fake broker in an fake client.

        The broker parameter is mandatory because it's important that cleanup
        happen. If ``None`` is passed in explicitly, a new broker object will
        be created.
        """
        spec = get_spec(vumi_resource_path("amqp-spec-0-8.xml"))
        return FakeAMQClient(spec, {}, broker)

    @classmethod
    def get_worker_raw(cls, worker_class, config, broker=None):
        """
        Create and return an instance of a vumi worker.

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
        """
        Create and return an instance of a vumi worker.

        :param worker_class: The worker class to instantiate.
        :param config: Config dict.
        :param start:
            ``True`` to start the worker (default), ``False`` otherwise.
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
        """
        Get messages dispatched to a routing key.

        The more specific :meth:`get_dispatched_events`,
        :meth:`get_dispatched_inbound`, and :meth:`get_dispatched_outbound`
        wrapper methods should be used instead where they apply.

        :param str connector_name:
            The connector name, which is used as the routing key prefix.

        :param str name:
            The routing key suffix, generally ``"event"``, ``"inbound"``, or
            ``"outbound"``.

        :param message_class:
            The message class to wrap the raw message data in. This should
            probably be :class:`~vumi.message.TransportUserMessage` or
            :class:`~vumi.message.TransportEvent`.
        """
        msgs = self.broker.get_dispatched(
            'vumi', self._rkey(connector_name, name))
        return [message_class.from_json(msg.body) for msg in msgs]

    def _wait_for_dispatched(self, connector_name, name, amount):
        rkey = self._rkey(connector_name, name)
        return self.broker.wait_messages('vumi', rkey, amount)

    @proxyable
    def clear_all_dispatched(self):
        """
        Clear all dispatched messages from the broker.
        """
        self.broker.clear_messages('vumi')
        self.broker.clear_messages('vumi.metrics')

    def _clear_dispatched(self, connector_name, name):
        rkey = self._rkey(connector_name, name)
        return self.broker.clear_messages('vumi', rkey)

    @proxyable
    def get_dispatched_events(self, connector_name=None):
        """
        Get events dispatched to a connector.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns: A list of :class:`~vumi.message.TransportEvent` instances.
        """
        return self.get_dispatched(connector_name, 'event', TransportEvent)

    @proxyable
    def get_dispatched_inbound(self, connector_name=None):
        """
        Get inbound messages dispatched to a connector.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A list of :class:`~vumi.message.TransportUserMessage` instances.
        """
        return self.get_dispatched(
            connector_name, 'inbound', TransportUserMessage)

    @proxyable
    def get_dispatched_outbound(self, connector_name=None):
        """
        Get outbound messages dispatched to a connector.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A list of :class:`~vumi.message.TransportUserMessage` instances.
        """
        return self.get_dispatched(
            connector_name, 'outbound', TransportUserMessage)

    @proxyable
    def wait_for_dispatched_events(self, amount, connector_name=None):
        """
        Wait for events dispatched to a connector.

        :param int amount:
            Number of events to wait for.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A :class:`Deferred` that fires with a list of
            :class:`~vumi.message.TransportEvent` instances.
        """
        d = self._wait_for_dispatched(connector_name, 'event', amount)
        d.addCallback(lambda msgs: [
            TransportEvent(**msg.payload) for msg in msgs])
        return d

    @proxyable
    def wait_for_dispatched_inbound(self, amount, connector_name=None):
        """
        Wait for inbound messages dispatched to a connector.

        :param int amount:
            Number of messages to wait for.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A :class:`Deferred` that fires with a list of
            :class:`~vumi.message.TransportUserMessage` instances.
        """
        d = self._wait_for_dispatched(connector_name, 'inbound', amount)
        d.addCallback(lambda msgs: [
            TransportUserMessage(**msg.payload) for msg in msgs])
        return d

    @proxyable
    def wait_for_dispatched_outbound(self, amount, connector_name=None):
        """
        Wait for outbound messages dispatched to a connector.

        :param int amount:
            Number of messages to wait for.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A :class:`Deferred` that fires with a list of
            :class:`~vumi.message.TransportUserMessage` instances.
        """
        d = self._wait_for_dispatched(connector_name, 'outbound', amount)
        d.addCallback(lambda msgs: [
            TransportUserMessage(**msg.payload) for msg in msgs])
        return d

    @proxyable
    def clear_dispatched_events(self, connector_name=None):
        """
        Clear dispatched events for a connector.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.
        """
        return self._clear_dispatched(connector_name, 'event')

    @proxyable
    def clear_dispatched_inbound(self, connector_name=None):
        """
        Clear dispatched inbound messages for a connector.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.
        """
        return self._clear_dispatched(connector_name, 'inbound')

    @proxyable
    def clear_dispatched_outbound(self, connector_name=None):
        """
        Clear dispatched outbound messages for a connector.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.
        """
        return self._clear_dispatched(connector_name, 'outbound')

    @proxyable
    def dispatch_raw(self, routing_key, message, exchange='vumi'):
        """
        Dispatch a message to the specified routing key.

        The more specific :meth:`dispatch_inbound`, :meth:`dispatch_outbound`,
        and :meth:`dispatch_event` wrapper methods should be used instead where
        they apply.

        :param str routing_key:
            Routing key to dispatch the message to.

        :param message:
            Message to dispatch.

        :param str exchange:
            AMQP exchange to dispatch the message to. Defaults to ``"vumi"``

        :returns:
            A :class:`Deferred` that fires when all messages have been
            delivered.
        """
        self.broker.publish_message(exchange, routing_key, message)
        return self.kick_delivery()

    @proxyable
    def dispatch_inbound(self, message, connector_name=None):
        """
        Dispatch an inbound message.

        :param message:
            Message to dispatch. Should be a
            :class:`~vumi.message.TransportUserMessage` instance.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A :class:`Deferred` that fires when all messages have been
            delivered.
        """
        return self.dispatch_raw(
            self._rkey(connector_name, 'inbound'), message)

    @proxyable
    def dispatch_outbound(self, message, connector_name=None):
        """
        Dispatch an outbound message.

        :param message:
            Message to dispatch. Should be a
            :class:`~vumi.message.TransportUserMessage` instance.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A :class:`Deferred` that fires when all messages have been
            delivered.
        """
        return self.dispatch_raw(
            self._rkey(connector_name, 'outbound'), message)

    @proxyable
    def dispatch_event(self, message, connector_name=None):
        """
        Dispatch an event.

        :param message:
            Message to dispatch. Should be a
            :class:`~vumi.message.TransportEvent` instance.

        :param str connector_name:
            Connector name. If ``None``, the default connector name for the
            helper instance will be used.

        :returns:
            A :class:`Deferred` that fires when all messages have been
            delivered.
        """
        return self.dispatch_raw(
            self._rkey(connector_name, 'event'), message)

    @proxyable
    def kick_delivery(self):
        """
        Trigger delivery of messages by the broker.

        This is generally called internally by anything that sends a message.

        :returns:
            A :class:`Deferred` that fires when all messages have been
            delivered.
        """
        return self.broker.kick_delivery()

    @proxyable
    def get_dispatched_metrics(self):
        """
        Get dispatched metrics.

        The list of datapoints from each dispatched metrics message is
        returned.
        """
        msgs = self.broker.get_dispatched('vumi.metrics', 'vumi.metrics')
        return [json.loads(msg.body)['datapoints'] for msg in msgs]

    @proxyable
    def clear_dispatched_metrics(self):
        """
        Clear dispatched metrics messages from the broker.
        """
        self.broker.clear_messages('vumi.metrics')


class MessageDispatchHelper(object):
    """
    Helper for creating and immediately dispatching messages.

    This builds on top of :class:`MessageHelper` and :class:`WorkerHelper`.

    It does not allow dispatching to nonstandard connectors. If you need to do
    that, either use :class:`MessageHelper` and :class:`WorkerHelper` directly
    or build a second :class:`MessageDispatchHelper` with a second
    :class:`WorkerHelper`.

    :param msg_helper: A :class:`MessageHelper` instance.
    :param worker_helper: A :class:`WorkerHelper` instance.
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
        """
        Construct and dispatch an inbound message.

        This is a wrapper around :meth:`MessageHelper.make_inbound` (to which
        all parameters are passed) and :meth:`WorkerHelper.dispatch_inbound`.

        :returns:
            A :class:`Deferred` that fires with the constructed message once it
            has been dispatched.
        """
        msg = self.msg_helper.make_inbound(*args, **kw)
        d = self.worker_helper.dispatch_inbound(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_outbound(self, *args, **kw):
        """
        Construct and dispatch an outbound message.

        This is a wrapper around :meth:`MessageHelper.make_outbound` (to which
        all parameters are passed) and :meth:`WorkerHelper.dispatch_outbound`.

        :returns:
            A :class:`Deferred` that fires with the constructed message once it
            has been dispatched.
        """
        msg = self.msg_helper.make_outbound(*args, **kw)
        d = self.worker_helper.dispatch_outbound(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_ack(self, *args, **kw):
        """
        Construct and dispatch an ack event.

        This is a wrapper around :meth:`MessageHelper.make_ack` (to which all
        parameters are passed) and :meth:`WorkerHelper.dispatch_event`.

        :returns:
            A :class:`Deferred` that fires with the constructed event once it
            has been dispatched.
        """
        msg = self.msg_helper.make_ack(*args, **kw)
        d = self.worker_helper.dispatch_event(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_nack(self, *args, **kw):
        """
        Construct and dispatch a nack event.

        This is a wrapper around :meth:`MessageHelper.make_nack` (to which all
        parameters are passed) and :meth:`WorkerHelper.dispatch_event`.

        :returns:
            A :class:`Deferred` that fires with the constructed event once it
            has been dispatched.
        """
        msg = self.msg_helper.make_nack(*args, **kw)
        d = self.worker_helper.dispatch_event(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_delivery_report(self, *args, **kw):
        """
        Construct and dispatch a delivery report event.

        This is a wrapper around :meth:`MessageHelper.make_delivery_report` (to
        which all parameters are passed) and
        :meth:`WorkerHelper.dispatch_event`.

        :returns:
            A :class:`Deferred` that fires with the constructed event once it
            has been dispatched.
        """
        msg = self.msg_helper.make_delivery_report(*args, **kw)
        d = self.worker_helper.dispatch_event(msg)
        return d.addCallback(lambda r: msg)

    @proxyable
    def make_dispatch_reply(self, *args, **kw):
        """
        Construct and dispatch a reply message.

        This is a wrapper around :meth:`MessageHelper.make_reply` (to which all
        parameters are passed) and :meth:`WorkerHelper.dispatch_outbound`.

        :returns:
            A :class:`Deferred` that fires with the constructed message once it
            has been dispatched.
        """
        msg = self.msg_helper.make_reply(*args, **kw)
        d = self.worker_helper.dispatch_outbound(msg)
        return d.addCallback(lambda r: msg)


class RiakDisabledForTest(object):
    """
    Placeholder object for a disabled riak config.

    This class exists to throw a meaningful error when trying to use Riak in
    a test that disallows it. We can't do this from inside the Riak setup
    infrastructure, because that would be very invasive for something that
    only really matters for tests.
    """
    def __getattr__(self, name):
        raise RuntimeError(
            "Use of Riak has been disabled for this test. Please set "
            "'use_riak = True' on the test class to enable it.")

    def __deepcopy__(self, memo):
        """
        We have no state, but ``deepcopy()`` triggers our :meth:`__getattr__`.
        We return ``self`` so the copy compares equal.
        """
        return self


def import_filter(exc, *expected):
    msg = exc.args[0]
    module = msg.split()[-1]
    if expected and (module not in expected):
        raise
    return module


def import_skip(exc, *expected):
    """
    Raise :class:`SkipTest` if the provided :class:`ImportError` matches a
    module name in ``expected``, otherwise reraise the :class:`ImportError`.

    This is useful for skipping tests that require optional dependencies which
    might not be present.
    """
    module = import_filter(exc, *expected)
    raise SkipTest("Failed to import '%s'." % (module,))


def maybe_async(sync_attr):
    """
    Decorate a method that may be sync or async.

    This redecorates with the either ``@inlineCallbacks`` or
    ``@flatten_generator``, depending on the value of ``sync_attr``.
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


def maybe_async_return(value, maybe_deferred):
    """
    Return ``value`` or a deferred that fires with it.

    This is useful in cases where we're performing a potentially async
    operation but don't necessarily have enough information to use
    `maybe_async`.
    """
    if isinstance(maybe_deferred, Deferred):
        return maybe_deferred.addCallback(lambda r: value)
    return value


class PersistenceHelper(object):
    """
    Test helper for managing persistent storage.

    This helper manages Riak and Redis clients and configs and cleans up after
    them. It does no setup, but its cleanup may take a while if there's a lot
    in Riak.

    All configs for objects that build Riak or Redis clients must be passed
    through :meth:`mk_config`.

    :param bool use_riak:
        Pass ``True`` if Riak is desired, otherwise it will be disabled in the
        generated config parameters.

    :param bool is_sync:
        Pass ``True`` if synchronous Riak and Redis clients are desired,
        otherwise asynchronous ones will be built. This only applies to clients
        built by this helper, not those built by other objects using configs
        from this helper.
    """

    implements(IHelper)

    _patches_applied = False

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

    def setup(self):
        self._patch_riak()
        self._patch_txriak()
        self._patch_redis()
        self._patch_txredis()
        self._patches_applied = True

    @maybe_async
    def cleanup(self):
        for purge, manager in self._get_riak_managers_for_cleanup():
            if purge:
                try:
                    yield self._purge_riak(manager)
                except ConnectionRefusedError:
                    pass
            yield manager.close_manager()

        for purge, manager in self._get_redis_managers_for_cleanup():
            if purge:
                yield self._purge_redis(manager)
            yield manager.close_manager()

        self._unpatch()

    def _get_riak_managers_for_cleanup(self):
        """
        Get a list of Riak managers and whether they should be purged.

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
        """
        Get a list of Redis managers and whether they should be purged.

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

    def _unpatch(self):
        for patch in reversed(self._patches):
            patch.restore()
        self._patches_applied = False

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
            import_filter(e, 'riak')
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

    def _check_patches_applied(self):
        if not self._patches_applied:
            raise Exception(
                "setup() must be called before performing this operation.")

    @proxyable
    def get_riak_manager(self, config=None):
        """
        Build and return a Riak manager.

        :param dict config:
            Riak manager config. (Not a complete worker config.) If ``None``,
            the one used by :meth:`mk_config` will be used.

        :returns:
            A :class:`~vumi.persist.riak_manager.RiakManager` or
            :class:`~vumi.persist.riak_manager.TxRiakManager`, depending on the
            value of :attr:`is_sync`.
        """
        self._check_patches_applied()
        if config is None:
            config = self._config_overrides['riak_manager'].copy()

        if self.is_sync:
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

    @proxyable
    def get_redis_manager(self, config=None):
        """
        Build and return a Redis manager.

        This will be backed by an in-memory fake unless the
        ``VUMITEST_REDIS_DB`` environment variable is set.

        :param dict config:
            Redis manager config. (Not a complete worker config.) If ``None``,
            the one used by :meth:`mk_config` will be used.

        :returns:
            A :class:`~vumi.persist.redis_manager.RedisManager` or
            :class:`~vumi.persist.redis_manager.TxRedisManager`, depending on
            the value of :attr:`is_sync`.
        """
        self._check_patches_applied()
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
        """
        Return a copy of ``config`` with the ``riak_manager`` and
        ``redis_manager`` fields overridden.

        All configs for things that create Riak or Redis clients should be
        passed through this method.
        """
        self._check_patches_applied()
        config = config.copy()
        config.update(self._config_overrides)
        return config
