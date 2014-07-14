from twisted.internet.defer import inlineCallbacks

from vumi.application.base import ApplicationWorker
from vumi.application.tests.helpers import ApplicationHelper
from vumi.tests.helpers import (
    VumiTestCase, IHelper, PersistenceHelper, MessageHelper, WorkerHelper,
    MessageDispatchHelper, success_result_of)


class RunningCheckApplication(ApplicationWorker):
    app_worker_running = False

    def setup_application(self):
        self.app_worker_running = True

    def teardown_application(self):
        self.app_worker_running = False


class FakeCleanupCheckHelper(object):
    cleaned_up = False

    def cleanup(self):
        self.cleaned_up = True


class TestApplicationHelper(VumiTestCase):
    def test_implements_IHelper(self):
        """
        ApplicationHelper instances should provide the IHelper interface.
        """
        self.assertTrue(IHelper.providedBy(ApplicationHelper(None)))

    def test_defaults(self):
        """
        ApplicationHelper instances should have the expected parameter
        defaults.
        """
        fake_app_class = object()
        app_helper = ApplicationHelper(fake_app_class)
        self.assertEqual(app_helper.application_class, fake_app_class)
        self.assertIsInstance(app_helper.persistence_helper, PersistenceHelper)
        self.assertIsInstance(app_helper.msg_helper, MessageHelper)
        self.assertIsInstance(app_helper.worker_helper, WorkerHelper)
        dispatch_helper = app_helper.dispatch_helper
        self.assertIsInstance(dispatch_helper, MessageDispatchHelper)
        self.assertEqual(dispatch_helper.msg_helper, app_helper.msg_helper)
        self.assertEqual(
            dispatch_helper.worker_helper, app_helper.worker_helper)
        self.assertEqual(app_helper.persistence_helper.use_riak, False)

    def test_all_params(self):
        """
        ApplicationHelper should pass use_riak to its PersistenceHelper and all
        other params to its MessageHelper.
        """
        fake_app_class = object()
        app_helper = ApplicationHelper(
            fake_app_class, use_riak=True, transport_addr='Obs station')
        self.assertEqual(app_helper.persistence_helper.use_riak, True)
        self.assertEqual(app_helper.msg_helper.transport_addr, 'Obs station')

    def test_setup_sync(self):
        """
        ApplicationHelper.setup() should return ``None``, not a Deferred.
        """
        msg_helper = ApplicationHelper(None)
        self.add_cleanup(msg_helper.cleanup)
        self.assertEqual(msg_helper.setup(), None)

    def test_cleanup(self):
        """
        ApplicationHelper.cleanup() should call .cleanup() on its
        PersistenceHelper and WorkerHelper.
        """
        app_helper = ApplicationHelper(None)
        app_helper.persistence_helper = FakeCleanupCheckHelper()
        app_helper.worker_helper = FakeCleanupCheckHelper()
        self.assertEqual(app_helper.persistence_helper.cleaned_up, False)
        self.assertEqual(app_helper.worker_helper.cleaned_up, False)
        success_result_of(app_helper.cleanup())
        self.assertEqual(app_helper.persistence_helper.cleaned_up, True)
        self.assertEqual(app_helper.worker_helper.cleaned_up, True)

    @inlineCallbacks
    def test_get_application_defaults(self):
        """
        .get_application() should return a started application worker.
        """
        app_helper = self.add_helper(
            ApplicationHelper(RunningCheckApplication))
        app = yield app_helper.get_application({})
        self.assertIsInstance(app, RunningCheckApplication)
        self.assertEqual(app.app_worker_running, True)

    @inlineCallbacks
    def test_get_application_no_start(self):
        """
        .get_application() should return an unstarted application worker if
        passed ``start=False``.
        """
        app_helper = self.add_helper(
            ApplicationHelper(RunningCheckApplication))
        app = yield app_helper.get_application({}, start=False)
        self.assertIsInstance(app, RunningCheckApplication)
        self.assertEqual(app.app_worker_running, False)

    @inlineCallbacks
    def test_get_application_different_class(self):
        """
        .get_application() should return an instance of the specified worker
        class if one is provided.
        """
        app_helper = self.add_helper(ApplicationHelper(ApplicationWorker))
        app = yield app_helper.get_application({}, cls=RunningCheckApplication)
        self.assertIsInstance(app, RunningCheckApplication)
