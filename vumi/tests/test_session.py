from twisted.trial.unittest import TestCase
from vumi.session import VumiSession, TemplatedDecisionTree, PopulatedDesicionTree, TraversedDecisionTree

class SessionTestCase(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_session_decision_tree(self):
        sess1 = VumiSession()
        sess2 = VumiSession()
        sess3 = VumiSession()
        dt1 = TemplatedDecisionTree()
        dt2 = PopulatedDesicionTree()
        dt3 = TraversedDecisionTree()
        sess1.set_decision_tree(dt1)
        sess2.set_decision_tree(dt2)
        sess3.set_decision_tree(dt3)
        # baby steps
        self.assertEquals(sess1.get_decision_tree(), dt1)
        self.assertEquals(sess2.get_decision_tree(), dt2)
        self.assertEquals(sess3.get_decision_tree(), dt3)
        # the new TraversedDecisionTree should not be completed
        self.assertFalse(dt3.is_completed())

        test_yaml = '''
        __start__: users

        users:
            question:
                english: "Which user are you?"
            options: name
            check: match
            action: select
            next: items
            fail: repeat

        items:
            question:
                english: "Which item?"
            options: name
            check: match
            action: select
            next: value
            fail: repeat

        value:
            question:
                english: "How much?"
            check: integer
            action: save
            next: value2
            fail: repeat

        value2:
            question:
                english: "How many?"
            check: integer
            action: save
            next: timestamp
            fail: repeat

        timestamp:
            question:
                english: "Was it today?"
            options:
                true:
                    display:
                        english: "yes"
                    action: save_now
                    next: finish
                false:
                    display:
                        english: "no"
                    question: "Please enter the day [yyyy/mm/dd] ?"
                    check: date
                    action: save
                    next: finish
                    fail: repeat
            check: match
            fail: repeat

        '''

        test_json = '''
        {
            "users": [
                {
                    "name": "Simon",
                    "items": [
                        {
                            "name": "alpha",
                            "value": 0,
                            "value2": 0,
                            "timestamp": 0,
                            "id": "1.1"
                        },
                        {
                            "name": "beta",
                            "value": 0,
                            "value2": 0,
                            "timestamp": 0,
                            "id": "1.2"
                        }
                    ],
                    "timestamp": "1234567890",
                    "id": "1"
                },
                {
                    "name": "David",
                    "items": [],
                    "timestamp": "1234567890",
                    "id": "2"
                }
            ],
            "msisdn": "12345"
        }
        '''

        # just check the load operations don't blow up
        self.assertEquals(dt1.load_yaml_template(test_yaml), None)
        self.assertEquals(dt2.load_yaml_template(test_yaml), None)
        self.assertEquals(dt2.load_json_data(test_json), None)
        self.assertEquals(dt3.load_yaml_template(test_yaml), None)
        self.assertEquals(dt3.load_json_data(test_json), None)

        dt3.start()


