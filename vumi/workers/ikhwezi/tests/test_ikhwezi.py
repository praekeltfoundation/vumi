import yaml
import random
import redis

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.transports.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.ikhwezi.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker)
from vumi.tests.utils import FakeRedis


class RedisInputSequenceTest(TestCase):

    def setUp(self):
        self.msisdn = '0821234567'
        self.quiz = yaml.load(QUIZ)
        self.translations = yaml.load(TRANSLATIONS)
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}
        self.set_ds()

    def get_quiz_entry(self):
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.ds,
                self.msisdn)
        return ik

    def tearDown(self):
        keys = self.ds.keys("vumi_vodacom_ikhwezi*")
        if len(keys) and type(keys) is str:
            keys = keys.split(' ')
        if len(keys):
            for k in keys:
                #print self.ds.get(k)
                self.ds.delete(k)

    def set_ds(self):
        self.ds = redis.Redis("localhost", db=7)

    def exit_text(self):
        return self.quiz['exit']['headertext']

    def runInputSequence(self, inputs, context=None):
        user_in = inputs.pop(0)
        #print ''
        #print context
        #print user_in
        ik = self.get_quiz_entry()
        resp = ik.formulate_response(context, user_in)
        #print resp
        if len(inputs):
            return self.runInputSequence(inputs, resp.context)
        return resp

    def finishInputSequence(self, inputs):
        resp = self.runInputSequence(inputs)
        final_headertext = resp.headertext
        exit_text = self.exit_text()
        self.assertTrue(final_headertext.endswith(exit_text))

    def testAnswerOutOfRange1(self):
        """
        The 3 is impossible
        """
        inputs = [None, 1, 3, 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerOutOfRange2(self):
        """
        The 22 is impossible
        """
        inputs = [None, 22, 1, 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType1(self):
        inputs = [None, 1, "is there a 3rd option?", 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType2(self):
        inputs = [None, 1, 55, 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType3(self):
        """
        Mismatches on Continue question auto continue
        """
        inputs = [None, 1, 1, "exit", 1, 2, 2]
        self.finishInputSequence(inputs)

    def testSequence1(self):
        inputs = [None, 'blah', 55, '1', 1, 55, 55, 'blah',
                '1', 1, 55, 'blah', 55, '1', 2, 2]
        self.finishInputSequence(inputs)

    def testSequence2(self):
        """
        This sequence answers all questions,
        forcing an exit response to the final continure question
        """
        inputs = [None, 1, '1', 55, 2, '1', 55, 1, '1',
                55, 2, 55, '2', 1, '1', 1, '1', 1, 55, '2',
                55, '1', 1, '1', 55, '2', 'bye']
        self.finishInputSequence(inputs)

    def testSequence3(self):
        inputs = [None, 'demo', '2', 55, 'demo', 55, 55, 55, 55,
                '2', 55, 'demo', '2', 55, 55, 'demo', '2', 1, 55,
                55, '1', 55, '2', 55, '2', 55, '2', 55, '2', 2]
        self.finishInputSequence(inputs)

    def testWithForcedOrder(self):
        self.get_quiz_entry().force_order([
            'demographic1',
            'question10',
            'demographic2',
            'question7',
            'demographic4',
            'question9',
            'demographic3',
            'question2',
            'question6',
            'question1',
            'question4',
            'question5',
            'question3',
            'question8'])
        inputs = [None]
        resp = self.runInputSequence(inputs)
        self.assertEqual(4, len(resp.option_list))
        inputs = [1]
        resp = self.runInputSequence(inputs, resp.context)
        self.assertEqual(2, len(resp.option_list))
        inputs = [1]
        resp = self.runInputSequence(inputs, resp.context)
        self.assertEqual(2, len(resp.option_list))
        inputs = [1]
        resp = self.runInputSequence(inputs, resp.context)
        #print resp
        self.assertEqual(2, len(resp.option_list))

        # simulate resume
        inputs = [None]
        resp = self.runInputSequence(inputs)
        #print resp
        self.assertEqual(2, len(resp.option_list))

        # and 3 more attempts
        inputs = [None]
        resp = self.runInputSequence(inputs)
        #print resp
        self.assertEqual(2, len(resp.option_list))
        inputs = [None]
        resp = self.runInputSequence(inputs)
        #print resp
        self.assertEqual(2, len(resp.option_list))
        inputs = [None]
        resp = self.runInputSequence(inputs)
        #print resp
        self.assertEqual(0, len(resp.option_list))


class FakeRedisInputSequenceTest(RedisInputSequenceTest):

    def tearDown(self):
        pass

    def set_ds(self):
        self.ds = FakeRedis()
