from twisted.internet.defer import inlineCallbacks

from vumi.dispatchers.endpoint_dispatchers import Dispatcher
from vumi.dispatchers.tests.helpers import DummyDispatcher, DispatcherHelper
from vumi.tests.helpers import (
    VumiTestCase, IHelper, PersistenceHelper, MessageHelper, WorkerHelper,
    MessageDispatchHelper, success_result_of)


class TestDummyDispatcher(VumiTestCase):
    def test_publish_inbound(self):
        """
        DummyDispatcher should have a fake inbound publisher that remembers
        messages.
        """
        dispatcher = DummyDispatcher({
            'transport_names': ['ri_conn'],
            'exposed_names': ['ro_conn'],
        })
        self.assertEqual(dispatcher.exposed_publisher['ro_conn'].msgs, [])
        dispatcher.publish_inbound_message('ro_conn', 'fake inbound')
        self.assertEqual(
            dispatcher.exposed_publisher['ro_conn'].msgs, ['fake inbound'])
        dispatcher.exposed_publisher['ro_conn'].clear()
        self.assertEqual(dispatcher.exposed_publisher['ro_conn'].msgs, [])

    def test_publish_outbound(self):
        """
        DummyDispatcher should have a fake outbound publisher that remembers
        messages.
        """
        dispatcher = DummyDispatcher({
            'transport_names': ['ri_conn'],
            'exposed_names': ['ro_conn'],
        })
        self.assertEqual(dispatcher.transport_publisher['ri_conn'].msgs, [])
        dispatcher.publish_outbound_message('ri_conn', 'fake outbound')
        self.assertEqual(
            dispatcher.transport_publisher['ri_conn'].msgs, ['fake outbound'])
        dispatcher.transport_publisher['ri_conn'].clear()
        self.assertEqual(dispatcher.transport_publisher['ri_conn'].msgs, [])

    def test_publish_event(self):
        """
        DummyDispatcher should have a fake event publisher that remembers
        messages.
        """
        dispatcher = DummyDispatcher({
            'transport_names': ['ri_conn'],
            'exposed_names': ['ro_conn'],
        })
        self.assertEqual(
            dispatcher.exposed_event_publisher['ro_conn'].msgs, [])
        dispatcher.publish_inbound_event('ro_conn', 'fake event')
        self.assertEqual(
            dispatcher.exposed_event_publisher['ro_conn'].msgs, ['fake event'])
        dispatcher.exposed_event_publisher['ro_conn'].clear()
        self.assertEqual(
            dispatcher.exposed_event_publisher['ro_conn'].msgs, [])


class RunningCheckDispatcher(Dispatcher):
    disp_worker_running = False

    def setup_dispatcher(self):
        self.disp_worker_running = True

    def teardown_dispatcher(self):
        self.disp_worker_running = False


class FakeCleanupCheckHelper(object):
    cleaned_up = False

    def cleanup(self):
        self.cleaned_up = True


class TestDispatcherHelper(VumiTestCase):
    def test_implements_IHelper(self):
        """
        DispatcherHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(DispatcherHelper(None)))

    def test_defaults(self):
        """
        DispatcherHelper instances should have the expected parameter defaults.
        """
        fake_disp_class = object()
        disp_helper = DispatcherHelper(fake_disp_class)
        self.assertEqual(disp_helper.dispatcher_class, fake_disp_class)
        self.assertIsInstance(
            disp_helper.persistence_helper, PersistenceHelper)
        self.assertIsInstance(disp_helper.msg_helper, MessageHelper)
        self.assertIsInstance(disp_helper.worker_helper, WorkerHelper)
        dispatch_helper = disp_helper.dispatch_helper
        self.assertIsInstance(dispatch_helper, MessageDispatchHelper)
        self.assertEqual(dispatch_helper.msg_helper, disp_helper.msg_helper)
        self.assertEqual(
            dispatch_helper.worker_helper, disp_helper.worker_helper)
        self.assertEqual(disp_helper.persistence_helper.use_riak, False)

    def test_all_params(self):
        """
        DispatcherHelper should pass use_riak to its PersistenceHelper and all
        other params to its MessageHelper.
        """
        fake_disp_class = object()
        disp_helper = DispatcherHelper(
            fake_disp_class, use_riak=True, transport_addr='Obs station')
        self.assertEqual(disp_helper.persistence_helper.use_riak, True)
        self.assertEqual(disp_helper.msg_helper.transport_addr, 'Obs station')

    def test_setup_sync(self):
        """
        DispatcherHelper.setup() should return ``None``, not a Deferred.
        """
        msg_helper = DispatcherHelper(None)
        self.add_cleanup(msg_helper.cleanup)
        self.assertEqual(msg_helper.setup(), None)

    def test_cleanup(self):
        """
        DispatcherHelper.cleanup() should call .cleanup() on its
        PersistenceHelper and WorkerHelper.
        """
        disp_helper = DispatcherHelper(None)
        disp_helper.persistence_helper = FakeCleanupCheckHelper()
        disp_helper.worker_helper = FakeCleanupCheckHelper()
        self.assertEqual(disp_helper.persistence_helper.cleaned_up, False)
        self.assertEqual(disp_helper.worker_helper.cleaned_up, False)
        success_result_of(disp_helper.cleanup())
        self.assertEqual(disp_helper.persistence_helper.cleaned_up, True)
        self.assertEqual(disp_helper.worker_helper.cleaned_up, True)

    @inlineCallbacks
    def test_get_dispatcher_defaults(self):
        """
        .get_dispatcher() should return a started dispatcher.
        """
        disp_helper = self.add_helper(DispatcherHelper(RunningCheckDispatcher))
        app = yield disp_helper.get_dispatcher({
            'receive_inbound_connectors': [],
            'receive_outbound_connectors': [],
        })
        self.assertIsInstance(app, RunningCheckDispatcher)
        self.assertEqual(app.disp_worker_running, True)

    @inlineCallbacks
    def test_get_dispatcher_no_start(self):
        """
        .get_dispatcher() should return an unstarted dispatcher if passed
        ``start=False``.
        """
        disp_helper = self.add_helper(DispatcherHelper(RunningCheckDispatcher))
        app = yield disp_helper.get_dispatcher({
            'receive_inbound_connectors': [],
            'receive_outbound_connectors': [],
        }, start=False)
        self.assertIsInstance(app, RunningCheckDispatcher)
        self.assertEqual(app.disp_worker_running, False)

    @inlineCallbacks
    def test_get_application_different_class(self):
        """
        .get_dispatcher() should return an instance of the specified worker
        class if one is provided.
        """
        disp_helper = self.add_helper(DispatcherHelper(Dispatcher))
        app = yield disp_helper.get_dispatcher({
            'receive_inbound_connectors': [],
            'receive_outbound_connectors': [],
        }, cls=RunningCheckDispatcher)
        self.assertIsInstance(app, RunningCheckDispatcher)

    def test_get_connector_helper(self):
        """
        .get_connector_helper() should return a DispatcherConnectorHelper
        instance for the provided connector name.
        """
        disp_helper = DispatcherHelper(None)
        dc_helper = disp_helper.get_connector_helper('barconn')
        self.assertEqual(dc_helper.msg_helper, disp_helper.msg_helper)
        self.assertEqual(dc_helper.worker_helper._connector_name, 'barconn')
        self.assertEqual(
            dc_helper.worker_helper.broker, disp_helper.worker_helper.broker)
