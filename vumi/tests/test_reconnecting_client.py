"""Tests for vumi.reconnecting_client."""

from twisted.internet import interfaces
from twisted.trial.unittest import TestCase
from zope.interface import implementer

from vumi.reconnecting_client import ReconnectingClientService


@implementer(interfaces.IStreamClientEndpoint)
class ClientTestEndpoint(object):
    def connect(self, factory):
        pass


class ReconnectingClientServiceTestCase(TestCase):
    def test_startService(self):
        e = ClientTestEndpoint()
        f = lambda: None
        s = ReconnectingClientService(e, f)
        s.startService()
        return s.stopService()

    def test_stopService(self):
        pass

    def test_clientConnected(self):
        pass

    def test_clientConnectionFailed(self):
        pass

    def test_clientConnectionLost(self):
        pass

    def test_retry(self):
        pass

    def test_resetDelay(self):
        pass

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


    def __test_parametrizedClock(self):
        """
        The clock used by L{ReconnectingClientFactory} can be parametrized, so
        that one can cleanly test reconnections.
        """
        clock = Clock()
        factory = ReconnectingClientFactory()
        factory.clock = clock

        factory.clientConnectionLost(FakeConnector(), None)
        self.assertEqual(len(clock.calls), 1)
