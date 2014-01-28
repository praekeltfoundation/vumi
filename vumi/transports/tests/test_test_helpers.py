from twisted.internet.defer import inlineCallbacks

from vumi.transports.base import Transport
from vumi.transports.failures import FailureMessage
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import (
    VumiTestCase, IHelper, PersistenceHelper, MessageHelper, WorkerHelper,
    MessageDispatchHelper, success_result_of)


class RunningCheckTransport(Transport):
    tx_worker_running = False

    def setup_transport(self):
        self.tx_worker_running = True

    def teardown_transport(self):
        self.tx_worker_running = False


class FakeCleanupCheckHelper(object):
    cleaned_up = False

    def cleanup(self):
        self.cleaned_up = True


class TestTransportHelper(VumiTestCase):
    def test_implements_IHelper(self):
        """
        TransportHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(TransportHelper(None)))

    def test_defaults(self):
        """
        TransportHelper instances should have the expected parameter defaults.
        """
        fake_tx_class = object()
        tx_helper = TransportHelper(fake_tx_class)
        self.assertEqual(tx_helper.transport_class, fake_tx_class)
        self.assertIsInstance(tx_helper.persistence_helper, PersistenceHelper)
        self.assertIsInstance(tx_helper.msg_helper, MessageHelper)
        self.assertIsInstance(tx_helper.worker_helper, WorkerHelper)
        dispatch_helper = tx_helper.dispatch_helper
        self.assertIsInstance(dispatch_helper, MessageDispatchHelper)
        self.assertEqual(dispatch_helper.msg_helper, tx_helper.msg_helper)
        self.assertEqual(
            dispatch_helper.worker_helper, tx_helper.worker_helper)
        self.assertEqual(tx_helper.persistence_helper.use_riak, False)

    def test_all_params(self):
        """
        TransportHelper should pass use_riak to its PersistenceHelper and all
        other params to its MessageHelper.
        """
        fake_tx_class = object()
        tx_helper = TransportHelper(
            fake_tx_class, use_riak=True, transport_addr='Obs station')
        self.assertEqual(tx_helper.persistence_helper.use_riak, True)
        self.assertEqual(tx_helper.msg_helper.transport_addr, 'Obs station')

    def test_setup_sync(self):
        """
        TransportHelper.setup() should return ``None``, not a Deferred.
        """
        msg_helper = TransportHelper(None)
        self.add_cleanup(msg_helper.cleanup)
        self.assertEqual(msg_helper.setup(), None)

    def test_cleanup(self):
        """
        TransportHelper.cleanup() should call .cleanup() on its
        PersistenceHelper and WorkerHelper.
        """
        tx_helper = TransportHelper(None)
        tx_helper.persistence_helper = FakeCleanupCheckHelper()
        tx_helper.worker_helper = FakeCleanupCheckHelper()
        self.assertEqual(tx_helper.persistence_helper.cleaned_up, False)
        self.assertEqual(tx_helper.worker_helper.cleaned_up, False)
        success_result_of(tx_helper.cleanup())
        self.assertEqual(tx_helper.persistence_helper.cleaned_up, True)
        self.assertEqual(tx_helper.worker_helper.cleaned_up, True)

    @inlineCallbacks
    def test_get_transport_defaults(self):
        """
        .get_transport() should return a started transport worker.
        """
        tx_helper = self.add_helper(TransportHelper(RunningCheckTransport))
        app = yield tx_helper.get_transport({})
        self.assertIsInstance(app, RunningCheckTransport)
        self.assertEqual(app.tx_worker_running, True)

    @inlineCallbacks
    def test_get_transport_no_start(self):
        """
        .get_transport() should return an unstarted transport worker if passed
        ``start=False``.
        """
        tx_helper = self.add_helper(TransportHelper(RunningCheckTransport))
        app = yield tx_helper.get_transport({}, start=False)
        self.assertIsInstance(app, RunningCheckTransport)
        self.assertEqual(app.tx_worker_running, False)

    @inlineCallbacks
    def test_get_application_different_class(self):
        """
        .get_transport() should return an instance of the specified worker
        class if one is provided.
        """
        tx_helper = self.add_helper(TransportHelper(Transport))
        app = yield tx_helper.get_transport({}, cls=RunningCheckTransport)
        self.assertIsInstance(app, RunningCheckTransport)

    def _add_to_dispatched(self, broker, rkey, msg):
        broker.exchange_declare('vumi', 'direct')
        broker.publish_message('vumi', rkey, msg)

    def test_get_dispatched_failures(self):
        """
        .get_dispatched_failures() should get failures dispatched by the
        transport.
        """
        tx_helper = TransportHelper(Transport)
        dispatched = tx_helper.get_dispatched_failures('fooconn')
        self.assertEqual(dispatched, [])
        msg = FailureMessage(
            message=tx_helper.msg_helper.make_outbound('foo').payload,
            failure_code=FailureMessage.FC_UNSPECIFIED, reason='sadness')
        self._add_to_dispatched(
            tx_helper.worker_helper.broker, 'fooconn.failures', msg)
        dispatched = tx_helper.get_dispatched_failures('fooconn')
        self.assertEqual(dispatched, [msg])

    def test_get_dispatched_failures_no_connector(self):
        """
        .get_dispatched_failures() should use the default connector if none is
        passed in.
        """
        tx_helper = TransportHelper(Transport, transport_name='fooconn')
        dispatched = tx_helper.get_dispatched_failures()
        self.assertEqual(dispatched, [])
        msg = FailureMessage(
            message=tx_helper.msg_helper.make_outbound('foo').payload,
            failure_code=FailureMessage.FC_UNSPECIFIED, reason='sadness')
        self._add_to_dispatched(
            tx_helper.worker_helper.broker, 'fooconn.failures', msg)
        dispatched = tx_helper.get_dispatched_failures()
        self.assertEqual(dispatched, [msg])
