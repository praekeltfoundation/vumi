import time
import json
import yaml
import redis

from twisted.trial.unittest import TestCase
from vumi.session import VumiSession, TemplatedDecisionTree, PopulatedDecisionTree, TraversedDecisionTree
from vumi.workers.session.worker import SessionConsumer, SessionPublisher, SessionWorker
from vumi.session import *

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
        __data__:
            url:
            username:
            password:
            json: >
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


        # just check the load operations don't blow up
        self.assertEquals(dt1.load_yaml_template(test_yaml), None)
        self.assertEquals(dt2.load_yaml_template(test_yaml), None)
        self.assertEquals(dt2.load_dummy_data(), None)
        self.assertEquals(dt3.load_yaml_template(test_yaml), None)
        self.assertEquals(dt3.load_dummy_data(), None)

        # simple backtracking test
        before = dt3.dumps()
        dt3.start()
        dt3.go_back()
        self.assertEquals(before, dt3.dumps())
        #dt3.set_language("swahili")

        # a fake interaction
        self.assertEquals(dt3.start(),
                'Hello.')
        self.assertEquals(dt3.question(),
                'Who are you?\n1. Simon\n2. David')
        dt3.answer(1)
        self.assertEquals(dt3.question(),
                'Which item?\n1. alpha\n2. beta')
        dt3.answer(1)
        self.assertEquals(dt3.question(),
                'How much stuff?')
        dt3.answer(42)
        self.assertEquals(dt3.question(),
                'How many things?')
        dt3.answer(23)
        dt3.go_up()
        self.assertEquals(dt3.question(),
                'Which item?\n1. alpha\n2. beta')
        dt3.answer(2)
        self.assertEquals(dt3.question(),
                'How much stuff?')
        dt3.answer(22)
        self.assertEquals(dt3.question(),
                'How many things?')
        dt3.answer(222)
        self.assertEquals(dt3.question(),
                'Which day was it?\n1. Today\n2. Yesterday\n3. An earlier day')
        dt3.answer(3)
        self.assertEquals(dt3.question(),
                'Which day was it [dd/mm/yyyy]?')
        dt3.answer("03/03/2011")
        self.assertEquals(dt3.finish(),
                'Thank you and goodbye.')
        #print dt3.dumps(level=2, serialize=yaml.dump)


        test_yaml = '''
        __data__:
            url:
            username:
            password:
            json: >
                {
                    "users": [
                        {
                            "name": "Simon",
                            "items": [
                                {   "name": "one"},
                                {   "name": "two"},
                                {   "name": "three"},
                                {   "name": "four"},
                                {   "name": "five"},
                                {   "name": "six"},
                                {   "name": "seven"},
                                {   "name": "eight"},
                                {   "name": "nine"},
                                {   "name": "ten"},
                                {   "name": "eleven"},
                                {   "name": "twelve"},
                                {   "name": "something that uses up lots of characters"},
                                {   "name": "and use up more characters"},
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
            more:
                english: "more items ..."
                swahili: "zaidi ya vitu ..."
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

        r_server = redis.Redis("localhost")
        sc = SessionConsumer(None)
        sc.set_yaml_template(test_yaml)
        sc.del_session("12345")
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.echo_on()
        #sc.gsdt("12345").set_language("swahili")
        dt4.start()
        dt4.question()
        dt4.answer(4)
        dt4.question()
        dt4.answer(1)
        dt4.question()
        dt4.answer(0)
        dt4.question()
        dt4.answer(0)
        dt4.question()
        dt4.answer(1)
        dt4.question()
        dt4.answer(42)
        dt4.question()
        dt4.answer(23)
        dt4.question()
        dt4.answer('earlier')
        dt4.question()
        dt4.answer(3)
        dt4.question()
        dt4.answer("03/03/2011")
        sess4.save()
        print repr(sc.post_back_json("12345") or '')
        dt4.finish()

        print ''
        #print repr(dt4.get_data_source())
        #print sess4.get_decision_tree().dump_json_data()


        #sess_d = yaml.dump(sc.get_session("12345"))
        #sess_l = yaml.load(sess_d)
        #self.assertEquals(sess_d, yaml.dump(sess_l))
        #print sess_d



        #print "\n\n"
        #time.sleep(2)
