import yaml
from twisted.trial.unittest import TestCase

from vumi.session import (VumiSession, TemplatedDecisionTree,
                          PopulatedDecisionTree, TraversedDecisionTree)
from vumi.workers.session.worker import SessionConsumer


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
                                {   "name":
                                    "something that uses up lots of characters"
                                    },
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

        sc = SessionConsumer(None)

        r_server = sc.r_server
        r_server.flushall()

        sc.set_yaml_template(test_yaml)
        sc.del_session("12345")
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        #dt4.echo_on()
        #sc.gsdt("12345").set_language("swahili")
        self.assertEquals(dt4.start(),
                "Hello.")
        self.assertEquals(dt4.question(),
                "Who are you?\n1. Simon\n2. David")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(4)
        self.assertEquals(dt4.question(),
                "Who are you?\n1. Simon\n2. David")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(1)
        self.assertEquals(dt4.question(),
                "Which item?\n1. one\n2. two\n3. three\n4. four\n5. five\n6."
                " six\n7. seven\n8. eight\n9. nine\n0. more items ...")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(0)
        self.assertEquals(dt4.question(),
                "Which item?\n1. ten\n2. eleven\n3. twelve\n4. something that"
                " uses up lots of characters\n5. and use up more"
                " characters\n0. more items ...")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(0)
        self.assertEquals(dt4.question(),
                "Which item?\n1. alpha\n2. beta")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(1)
        self.assertEquals(dt4.question(),
                "How much stuff?")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(42)
        self.assertEquals(dt4.question(),
                "How many things?")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(23)

        self.assertEquals(dt4.question(),
                "Which day was it?\n1. Today\n2. Yesterday\n3. An earlier day")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer('earlier')
        self.assertEquals(dt4.question(),
                "Which day was it?\n1. Today\n2. Yesterday\n3. An earlier day")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer(3)
        self.assertEquals(dt4.question(),
                "Which day was it [dd/mm/yyyy]?")
        sess4.save()
        sess4 = None
        # after persisting to redis, retrieve afresh
        sess4 = sc.get_session("12345")
        dt4 = sess4.get_decision_tree()
        dt4.answer("03/03/2011")
        sess4.save()
        #print repr(sc.post_back_json("12345") or '')
        self.assertEquals(dt4.finish(),
                "Thank you and goodbye.")
        sess4.delete()
        sess4.save()

        #print r_server.info()
        #print r_server.keys()

        #r0 = redis.Redis("localhost", db=0)
        #r7 = redis.Redis("localhost", db=7)
        #r9 = redis.Redis("localhost", db=9)
        #r0.flushall()
        #r7.flushall()
        #r9.flushall()
        #r0.set('a','a')
        #r7.set('a','a')
        #r9.set('c','c')
        #print r0.info()
        #print r0.keys()
        #print r7.info()
        #print r7.keys()
        #print r9.info()
        #print r9.keys()
        #r0.flushall()
        #r7.flushall()
        #r9.flushall()

        #print "\n\n"
        #time.sleep(2)


class TemplatedDecisionTreeTestCase(TestCase):
    """
    Tests for `TemplatedDecisionTree`.
    """

    def test_load_yaml_template_unsafe(self):
        """
        `load_yaml_template()` should not allow unsafe YAML tag execution.
        """
        dt = TemplatedDecisionTree()
        self.assertRaises(yaml.constructor.ConstructorError,
            lambda: dt.load_yaml_template('!!python/object/apply:int []'))
        # These attributes should not have been set.
        self.assertIdentical(dt.template, None)
        self.assertIdentical(dt.template_current, None)
