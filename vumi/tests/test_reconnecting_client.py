"""Tests for vumi.reconnecting_client."""

import random

from twisted.internet import interfaces
from twisted.internet.defer import inlineCallbacks, Deferred, CancelledError
from twisted.internet.protocol import Protocol
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.trial.unittest import TestCase
from zope.interface import implementer

from vumi.reconnecting_client import ReconnectingClientService
from vumi.tests.utils import LogCatcher


@implementer(interfaces.IStreamClientEndpoint)
class ClientTestEndpoint(object):
    def __init__(self):
        self.connect_called = Deferred()
        self.connected = Deferred()

    def connect(self, factory):
        self.connect_called.callback(factory)
        return self.connected


class DummyProtocol(Protocol):
    pass


@implementer(interfaces.ITransport)
class DummyTransport(object):

    def __init__(self):
        self.lose_connection_called = Deferred()

    def loseConnection(self):
        self.lose_connection_called.callback(None)


class MockRecorder(object):
    def __init__(self, test_case, result=None):
        self._test_case = test_case
        self._calls = []
        self._result = result

    def assertCalledOnce(self, *args, **kw):
        self._test_case.assertEqual(self._calls, [(args, kw)])

    def assertNotCalled(self):
        self._test_case.assertEqual(self._calls, [])

    def __call__(self, *args, **kw):
        self._calls.append((args, kw))
        return self._result


class ReconnectingClientServiceTestCase(TestCase):
    def make_reconnector(self, **kw):
        e = ClientTestEndpoint()
        f = object()
        s = ReconnectingClientService(e, f)
        for key, value in kw.items():
            setattr(s, key, value)
        self.addCleanup(s.stopService)
        return s, e, f

    def patch_reconnector(self, method):
        mock = MockRecorder(self)
        self.patch(ReconnectingClientService, method, mock)
        return mock

    def test_startService(self):
        retry = self.patch_reconnector('retry')
        s = ReconnectingClientService(object(), object())
        s.startService()
        self.assertTrue(s.continueTrying)
        retry.assertCalledOnce(delay=0.0)

    @inlineCallbacks
    def test_stopService(self):
        s, e, f = self.make_reconnector(continueTrying=True)
        yield s.stopService()
        self.assertEqual(s.continueTrying, False)

    @inlineCallbacks
    def test_stopService_while_retrying(self):
        s, e, f = self.make_reconnector()
        clock = Clock()
        r = s._delayedRetry = clock.callLater(1.0, lambda: None)
        yield s.stopService()
        self.assertTrue(r.cancelled)
        self.assertIdentical(s._delayedRetry, None)

    @inlineCallbacks
    def test_stopService_while_connecting(self):
        errs = []
        s, e, f = self.make_reconnector()
        s._connectingDeferred = Deferred().addErrback(errs.append)
        yield s.stopService()
        [failure] = errs
        self.assertTrue(failure.check(CancelledError))

    @inlineCallbacks
    def test_stopService_while_connected(self):
        s, e, f = self.make_reconnector()
        s._protocol = DummyProtocol()
        s._protocol.transport = DummyTransport()
        d = s.stopService()
        self.assertFalse(d.called)
        self.assertTrue(s._protocol.transport.lose_connection_called.called)
        s.clientConnectionLost(Failure(Exception()))
        yield d

    def test_clientConnected(self):
        reset = self.patch_reconnector('resetDelay')
        s = ReconnectingClientService(object(), object())
        p = object()
        s.clientConnected(p)
        self.assertIdentical(s._protocol, p)
        reset.assertCalledOnce()

    def test_clientConnectionFailed(self):
        retry = self.patch_reconnector('retry')
        s = ReconnectingClientService(object(), object())
        s.clientConnectionFailed(Failure(Exception()))
        self.assertIdentical(s._protocol, None)
        retry.assertCalledOnce()

    def test_clientConnectionLost(self):
        retry = self.patch_reconnector('retry')
        s = ReconnectingClientService(object(), object())
        s.clientConnectionLost(Failure(Exception()))
        self.assertIdentical(s._protocol, None)
        retry.assertCalledOnce()

    def test_clientConnectionLost_while_stopping(self):
        retry = self.patch_reconnector('retry')
        s = ReconnectingClientService(object(), object())
        d = s._protocolStoppingDeferred = Deferred()
        s.clientConnectionLost(Failure(Exception()))
        self.assertIdentical(s._protocol, None)
        self.assertIdentical(s._protocolStoppingDeferred, None)
        retry.assertCalledOnce()
        self.assertTrue(d.called)

    def test_retry_aborts_when_stopping(self):
        s, e, f = self.make_reconnector(continueTrying=False)
        s.retry()
        self.assertEqual(s.retries, 0)

    def test_noisy_retry_aborts_when_stopping(self):
        s, e, f = self.make_reconnector(noisy=True, continueTrying=False)
        with LogCatcher() as lc:
            s.retry()
            [msg] = lc.messages()
        self.assertEqual(s.retries, 0)
        self.assertSubstring("Abandoning <vumi.tests.test_reconnecting_client"
                             ".ClientTestEndpoint object at", msg)
        self.assertSubstring("on explicit request", msg)

    def test_retry_aborts_when_max_retries_exceeded(self):
        s, e, f = self.make_reconnector(maxRetries=5, continueTrying=True)
        s.retries = 5
        s.retry()
        self.assertEqual(s.retries, 5)

    def test_noisy_retry_aborts_when_max_retries_exceeded(self):
        s, e, f = self.make_reconnector(noisy=True, maxRetries=5,
                                        continueTrying=True)
        s.retries = 5
        with LogCatcher() as lc:
            s.retry()
            [msg] = lc.messages()
        self.assertEqual(s.retries, 5)
        self.assertSubstring("Abandoning <vumi.tests.test_reconnecting_client"
                             ".ClientTestEndpoint object at", msg)
        self.assertSubstring("after 5 retries", msg)

    def test_retry_with_explicit_delay(self):
        s, e, f = self.make_reconnector(continueTrying=True, clock=Clock())
        s.retry(delay=1.5)
        [delayed] = s.clock.calls
        self.assertEqual(delayed.time, 1.5)

    def test_noisy_retry_with_explicit_delay(self):
        s, e, f = self.make_reconnector(noisy=True, continueTrying=True,
                                        clock=Clock())
        with LogCatcher() as lc:
            s.retry(delay=1.5)
            [msg] = lc.messages()
        [delayed] = s.clock.calls
        self.assertEqual(delayed.time, 1.5)
        self.assertSubstring("Will retry <vumi.tests.test_reconnecting_client"
                             ".ClientTestEndpoint object at", msg)
        self.assertSubstring("in 1.5 seconds", msg)

    def test_retry_delay_advances(self):
        s, e, f = self.make_reconnector(jitter=None, continueTrying=True,
                                        clock=Clock())
        s.retry()
        [delayed] = s.clock.calls
        self.assertAlmostEqual(delayed.time, s.factor)
        self.assertAlmostEqual(s.delay, s.factor)

    def test_retry_delay_is_capped_by_maxDelay(self):
        s, e, f = self.make_reconnector(jitter=None, continueTrying=True,
                                        clock=Clock(), maxDelay=1.5)
        s.retry()
        [delayed] = s.clock.calls
        self.assertAlmostEqual(delayed.time, 1.5)
        self.assertAlmostEqual(s.delay, 1.5)

    def test_retry_with_jitter(self):
        normal = MockRecorder(self, result=2.0)
        self.patch(random, 'normalvariate', normal)
        s, e, f = self.make_reconnector(continueTrying=True, clock=Clock())
        s.retry()
        [delayed] = s.clock.calls
        self.assertAlmostEqual(delayed.time, 2.0)
        self.assertAlmostEqual(s.delay, 2.0)
        normal.assertCalledOnce(s.factor, s.factor * s.jitter)

    @inlineCallbacks
    def test_retry_when_connection_succeeds(self):
        connected = self.patch_reconnector('clientConnected')
        s, e, f = self.make_reconnector(continueTrying=True, clock=Clock())

        s.retry(delay=1.0)
        connected.assertNotCalled()

        s.clock.advance(1.0)
        wrapped_f = yield e.connect_called
        self.assertEqual(wrapped_f.protocolFactory, f)
        connected.assertNotCalled()

        p = DummyProtocol()
        e.connected.callback(p)
        connected.assertCalledOnce(p)

    @inlineCallbacks
    def test_retry_when_connection_fails(self):
        connection_failed = self.patch_reconnector('clientConnectionFailed')
        s, e, f = self.make_reconnector(continueTrying=True, clock=Clock())

        s.retry(delay=1.0)
        connection_failed.assertNotCalled()

        s.clock.advance(1.0)
        wrapped_f = yield e.connect_called
        self.assertEqual(wrapped_f.protocolFactory, f)
        connection_failed.assertNotCalled()

        failure = Failure(Exception())
        e.connected.errback(failure)
        connection_failed.assertCalledOnce(failure)

    def test_resetDelay(self):
        initial_delay = ReconnectingClientService.initialDelay
        s = ReconnectingClientService(object(), object())
        s.delay, s.retries = initial_delay + 1, 5
        s.resetDelay()
        self.assertEqual(s.delay, initial_delay)
        self.assertEqual(s.retries, 0)

    def test_parametrizedClock(self):
        """
        The clock used by L{ReconnectingClientFactory} can be parametrized, so
        that one can cleanly test reconnections.
        """
        clock = Clock()
        s, e, f = self.make_reconnector()
        s.clock = clock
        s.startService()
        self.assertEqual(len(clock.calls), 1)
