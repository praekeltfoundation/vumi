import yaml
import random
import redis

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.transports.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.ikhwezi.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker)
from vumi.tests.utils import FakeRedis


trans = yaml.load(TRANSLATIONS)
for t in trans:
    print ''
    for k, v in t.items():
        print k.ljust(12,'.'), v

translations = {'Zulu': {}, 'Sotho': {}, 'Afrikaans': {}}
for t in trans:
    translations['Zulu'][t['English']] = t['Zulu']
    translations['Sotho'][t['English']] = t['Sotho']
    translations['Afrikaans'][t['English']] = t['Afrikaans']

for k,v in translations.items():
    print ''
    print k
    for kk,vv in v.items():
        print '--->', kk
        print ' L->', vv



class RedisInputSequenceTest(TestCase):

    def setUp(self):
        self.msisdn = '0821234567'
        self.session_event = 'new'
        self.provider = 'test'
        self.quiz = yaml.load(QUIZ)
        trans = yaml.load(TRANSLATIONS)
        self.translations = {'Zulu': {}, 'Sotho': {}, 'Afrikaans': {}}
        for t in trans:
            self.translations['Zulu'][t['English']] = t['Zulu']
            self.translations['Sotho'][t['English']] = t['Sotho']
            self.translations['Afrikaans'][t['English']] = t['Afrikaans']
        self.language = 'English'
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
                self.msisdn,
                self.session_event,
                self.provider)
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

    def runInputSequence(self, inputs, force_order=None):
        user_in = inputs.pop(0)
        print ''
        print self.session_event
        print user_in
        ik = self.get_quiz_entry()
        if force_order:
            ik.force_order(force_order)
        self.session_event = 'resume'
        #print self.session_event
        resp = ik.formulate_response(user_in)
        self.language = ik.language
        print resp
        if len(inputs):
            return self.runInputSequence(inputs)
        return resp

    def finishInputSequence(self, inputs):
        resp = self.runInputSequence(inputs)
        final_headertext = resp.headertext
        exit_text = self.exit_text()
        #self.assertTrue(final_headertext.endswith(exit_text))

    def testAnswerOutOfRange1(self):
        """
        The 66 is impossible
        """
        inputs = ['*120*11223344#', 1, 66, 1, 1, 1, 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerOutOfRange2(self):
        """
        The 22 is impossible
        """
        inputs = ['*120*11223344#', 22, 2, 2, 2, 2, 2, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType1(self):
        inputs = ['*120*11223344#', 1, "is there a 3rd option?", 1, 1, 1, 1, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType2(self):
        inputs = ['*120*11223344#', 1, '*120*11223344#', 1, 1, 1, 2, 2]
        self.finishInputSequence(inputs)

    def testAnswerWrongType3(self):
        """
        Mismatches on Continue question auto continue
        """
        inputs = ['*120*11223344#', 1, 1, "huh", 1, 2, 1, 'exit', 1, 2]
        self.finishInputSequence(inputs)

    def testSequence1(self):
        inputs = ['*120*11223344#', 'blah', 55, '1', 1, 55, 55, 'blah',
                '1', 1, 55, 'blah', 55, '1', 2, 2, 2]
        self.finishInputSequence(inputs)

    def testSequence2(self):
        """
        This sequence answers all questions,
        forcing an exit response to the final continure question
        """
        inputs = ['*120*11223344#', 1, '1', 55, 2, '1', 55, 1, '1',
                55, 2, 55, '2', 1, '1', 1, '1', 1, 55, '2',
                55, '1', 1, '1', 55, '2', 'bye', 'no', 1, 2]
        self.finishInputSequence(inputs)

    def testSequence3(self):
        inputs = ['*120*11223344#', 'demo', '2', 55, 'demo', 55, 55, 55, 55,
                '2', 55, 'demo', '2', 55, 55, 'demo', '2', 1, 55,
                55, '1', 55, '2', 55, '2', 55, '2', 55, '2', 2]
        self.finishInputSequence(inputs)

    #def testWithForcedOrder(self):
        #force_order = [
            #'demographic1',
            #'demographic2',
            #'demographic3',
            #'demographic4',
            #'question7',
            #'continue',
            #'question9',
            #'continue',
            #'question10',
            #'continue',
            #'question2',
            #'continue',
            #'question6',
            #'continue',
            #'question1',
            #'continue',
            #'question4',
            #'continue',
            #'question5',
            #'continue',
            #'question3',
            #'continue',
            #'question8',
            #'continue'
            #]
        #inputs = ['*120*11223344#']
        #resp = self.runInputSequence(inputs, force_order)
        #self.assertEqual(4, len(resp.option_list))
        #inputs = [1]
        #resp = self.runInputSequence(inputs)
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [1]
        #resp = self.runInputSequence(inputs)
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [1]
        #resp = self.runInputSequence(inputs)
        ##print resp
        #self.assertEqual(2, len(resp.option_list))

        ## simulate resume
        #inputs = ['*120*11223344#']
        #self.session_event = 'new'
        #resp = self.runInputSequence(inputs)
        ##print resp
        #self.assertEqual(2, len(resp.option_list))

        ## and 3 more attempts
        #inputs = [1]
        #self.session_event = 'new'
        #resp = self.runInputSequence(inputs)
        ##print resp
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [1]
        #self.session_event = 'new'
        #resp = self.runInputSequence(inputs)
        ##print resp
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [None]
        #self.session_event = 'new'
        #resp = self.runInputSequence(inputs)
        ##print resp
        #self.assertEqual(0, len(resp.option_list))


class FakeRedisInputSequenceTest(RedisInputSequenceTest):

    def tearDown(self):
        pass

    def set_ds(self):
        self.ds = FakeRedis()
