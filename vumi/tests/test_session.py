from twisted.trial.unittest import TestCase
from vumi.session import VumiSession, TemplatedDecisionTree, PopulatedDesicionTree, TraversedDecisionTree

class SessionTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_set_get_decision_tree(self):
        sess1 = VumiSession()
        sess2 = VumiSession()
        dt1 = TraversedDecisionTree()
        dt2 = PopulatedDesicionTree()
        sess1.set_decision_tree(dt1)
        sess2.set_decision_tree(dt2)
        # baby steps
        self.assertEquals(sess1.get_decision_tree(), dt1)
        self.assertEquals(sess2.get_decision_tree(), dt2)

