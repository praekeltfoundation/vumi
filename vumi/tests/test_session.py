import time
import yaml
from twisted.trial.unittest import TestCase
from vumi.session import VumiSession, TemplatedDecisionTree, PopulatedDecisionTree, TraversedDecisionTree

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
        dt2 = PopulatedDecisionTree()
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
        __start__:
            display:
                english: "Hello."
                swahili: "Salamu."
            next: users

        users:
            question:
                english: "Who are you?"
                swahili: "Ninyi ni nani?"
            options: name
            next: items

        items:
            question:
                english: "Which item?"
                swahili: "Ambayo kitu?"
            options: name
            next: stuff
            new:
                name:
                stuff: 0
                things: 0
                timestamp: 0
                id:

        stuff:
            question:
                english: "How much stuff?"
                swahili: "Kiasi gani stuff?"
            validate: integer
            next: things

        things:
            question:
                english: "How many things?"
                swahili: "Mambo mangapi?"
            validate: integer
            next: timestamp

        timestamp:
            question:
                english: "Which day was it?"
                swahili: "Siku ambayo ilikuwa ni?"
            options:
                  - display:
                        english: "Today"
                        swahili: "Leo"
                    default: today
                    next: __finish__
                  - display:
                        english: "Yesterday"
                        swahili: "Jana"
                    default: yesterday
                    next: __finish__
                  - display:
                        english: "An earlier day"
                        swahili: "Mapema siku ya"
                    next:
                        question:
                            english: "Which day was it [dd/mm/yyyy]?"
                            swahili: "Kuwaambia ambayo siku [dd/mm/yyyy]?"
                        validate: date
                        next: __finish__

        __finish__:
            display:
                english: "Thank you and goodbye."
                swahili: "Asante na kwaheri."
        '''

        test_json = '''
        {
            "users": [
                {
                    "name": "Simon",
                    "items": [
                        {
                            "name": "alpha",
                            "stuff": 0,
                            "things": 0,
                            "timestamp": 0,
                            "id": "1.1"
                        },
                        {
                            "name": "beta",
                            "stuff": 0,
                            "things": 0,
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

        dt3.echo_on()
        before = dt3.dumps()
        dt3.start()
        # simple backtracking test
        dt3.go_back()
        self.assertEquals(before, dt3.dumps())
        #dt3.set_language("swahili")
        dt3.start()
        dt3.question()
        dt3.answer(1)
        dt3.question()
        dt3.answer(1)
        dt3.question()
        dt3.answer(42)
        dt3.question()
        dt3.answer(23)
        dt3.go_up()
        dt3.question()
        dt3.answer(2)
        dt3.question()
        dt3.answer(22)
        dt3.question()
        dt3.answer(222)
        dt3.question()
        dt3.answer(3)
        dt3.question()
        dt3.answer("03/03/2011")
        dt3.finish()
        print dt3.dumps(level=2, serialize=yaml.dump)






        print "\n\n"
        time.sleep(2)
