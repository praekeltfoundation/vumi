"""Tests for vumi.reconnecting_client."""

from twisted.internet import interfaces
from twisted.internet.defer import inlineCallbacks, succeed, Deferred
from twisted.internet.task import Clock
from twisted.python.failure import Failure
from twisted.trial.unittest import TestCase
from zope.interface import implementer

from vumi.reconnecting_client import ReconnectingClientService


@implementer(interfaces.IStreamClientEndpoint)
class ClientTestEndpoint(object):
    def __init__(self):
        self.connect_called = Deferred()
        self.connected = Deferred()

    def connect(self, factory):
        self.connect_called.callback(factory)
        return self.connected


class MockRecorder(object):
    def __init__(self, test_case):
        self._test_case = test_case
        self._calls = []

    def assertCalledOnce(self, *args, **kw):
        self._test_case.assertEqual(self._calls, [(args, kw)])

    def __call__(self, *args, **kw):
        self._calls.append((args, kw))


class ReconnectingClientServiceTestCase(TestCase):
    def make_reconnector(self):
        e = ClientTestEndpoint()
        f = object()
        s = ReconnectingClientService(e, f)
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

    def test_stopService(self):
        # TODO: ...
        pass

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

    def test_retry(self):
        # TODO: ...
        pass

    def test_resetDelay(self):
        initial_delay = ReconnectingClientService.initialDelay
        s = ReconnectingClientService(object(), object())
        s.delay, s.retries = initial_delay + 1, 5
        s.resetDelay()
        self.assertEqual(s.delay, initial_delay)
        self.assertEqual(s.retries, 0)

    def __test_stopTryingWhenConnected(self):
        """
        If a L{ReconnectingClientFactory} has C{stopTrying} called while it is
        connected, it does not subsequently attempt to reconnect if the
        connection is later lost.
        """
        class NoConnectConnector(object):
            def stopConnecting(self):
                raise RuntimeError("Shouldn't be called, we're connected.")
            def connect(self):
                raise RuntimeError("Shouldn't be reconnecting.")

        c = ReconnectingClientFactory()
        c.protocol = Protocol
        # Let's pretend we've connected:
        c.buildProtocol(None)
        # Now we stop trying, then disconnect:
        c.stopTrying()
        c.clientConnectionLost(NoConnectConnector(), None)
        self.assertFalse(c.continueTrying)


    def __test_stopTryingDoesNotReconnect(self):
        """
        Calling stopTrying on a L{ReconnectingClientFactory} doesn't attempt a
        retry on any active connector.
        """
        class FactoryAwareFakeConnector(FakeConnector):
            attemptedRetry = False

            def stopConnecting(self):
                """
                Behave as though an ongoing connection attempt has now
                failed, and notify the factory of this.
                """
                f.clientConnectionFailed(self, None)

            def connect(self):
                """
                Record an attempt to reconnect, since this is what we
                are trying to avoid.
                """
                self.attemptedRetry = True

        f = ReconnectingClientFactory()
        f.clock = Clock()

        # simulate an active connection - stopConnecting on this connector should
        # be triggered when we call stopTrying
        f.connector = FactoryAwareFakeConnector()
        f.stopTrying()

        # make sure we never attempted to retry
        self.assertFalse(f.connector.attemptedRetry)
        self.assertFalse(f.clock.getDelayedCalls())

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
