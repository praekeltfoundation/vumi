import yaml
import random

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.vodacommessaging.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker)
from vumi.tests.utils import FakeRedis


class InputSequenceTest(TestCase):

    def setUp(self):
        self.msisdn = '0821234567'
        self.quiz = yaml.load(QUIZ)
        self.translations = yaml.load(TRANSLATIONS)
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}
        self.ds = FakeRedis()

    def get_quiz_entry(self):
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.ds,
                self.msisdn)
        return ik

    def exit_text(self):
        return self.quiz['exit']['headertext']

    def tearDown(self):
        pass

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
        inputs = [None, 1, None, 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType3(self):
        """
        Mismatches on Continue question auto continue
        """
        inputs = [None, 1, 1, "exit", 1, 2, 2]
        self.finishInputSequence(inputs)

    def testSequence1(self):
        inputs = [None, 'blah', None, '1', 1, None, None, 'blah',
                '1', 1, None, 'blah', None, '1', 2, 2]
        self.finishInputSequence(inputs)

    def testSequence2(self):
        """
        This sequence answers all questions,
        forcing an exit response to the final continure question
        """
        inputs = [None, 1, '1', None, 2, '1', None, 1, '1',
                None, 2, None, '2', 1, '1', 1, '1', 1, None, '2',
                None, '1', 1, '1', None, '2', 'bye']
        self.finishInputSequence(inputs)

    def testSequence3(self):
        inputs = [None, 'demo', '2', None, 'demo', None, None, None, None,
                '2', None, 'demo', '2', None, None, 'demo', '2', 1, None,
                None, '1', None, '2', None, '2', None, '2', None, '2', 2]
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

