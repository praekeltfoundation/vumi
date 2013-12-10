from twisted.internet.defer import inlineCallbacks

from vumi.tests.helpers import VumiTestCase
from vumi.application.tests.helpers import ApplicationHelper

from vumi.demos.calculator import CalculatorApp


class TestCalculatorApp(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.app_helper = ApplicationHelper(CalculatorApp)
        self.add_cleanup(self.app_helper.cleanup)
        self.worker = yield self.app_helper.get_application({})

    def test_something(self):
        self.assertTrue(self.worker)
