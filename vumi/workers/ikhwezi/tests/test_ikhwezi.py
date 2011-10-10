import yaml
import random
import redis

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.transports.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.ikhwezi.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker)
from vumi.tests.utils import FakeRedis


#trans = yaml.load(TRANSLATIONS)
#for t in trans:
    #print ''
    #for k, v in t.items():
        #print k.ljust(12,'.'), v

#translations = {'Zulu': {}, 'Sotho': {}, 'Afrikaans': {}}
#for t in trans:
    #translations['Zulu'][t['English']] = t['Zulu']
    #translations['Sotho'][t['English']] = t['Sotho']
    #translations['Afrikaans'][t['English']] = t['Afrikaans']

#for k,v in translations.items():
    #print ''
    #print k
    #for kk,vv in v.items():
        #print '--->', kk
        #print ' L->', vv



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
        self.exit_text = None
        self.completed_text = None
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

    def set_exit_text(self, fun):
        self.exit_text = fun(self.quiz['exit']['headertext'])

    def set_completed_text(self, fun):
        self.completed_text = fun(self.quiz['completed']['headertext'])

    def run_input_sequence(self, inputs, force_order=None):
        user_in = inputs.pop(0)
        #print ''
        #print self.session_event
        #print user_in
        ik = self.get_quiz_entry()
        if force_order:
            ik.force_order(force_order)
        self.session_event = 'resume'
        #print self.session_event
        resp = ik.formulate_response(user_in)
        self.language = ik.language
        self.set_completed_text(ik._)
        self.set_exit_text(ik._)
        #print resp
        if len(inputs):
            return self.run_input_sequence(inputs)
        return resp

    def finish_input_sequence(self, inputs):
        resp = self.run_input_sequence(inputs)
        final_headertext = resp.headertext
        ref_text = self.exit_text
        self.assertTrue(final_headertext.endswith(ref_text))

    def completed_input_sequence(self, inputs):
        resp = self.run_input_sequence(inputs)
        final_headertext = resp.headertext
        ref_text = self.completed_text
        self.assertTrue(final_headertext.endswith(ref_text))

    def test_answer_out_of_range_1(self):
        """
        The 66 is impossible
        """
        inputs = ['*120*112233#', 1, 66, 1, 1, 1, 1, 2]
        self.finish_input_sequence(inputs)

    def test_answer_out_of_range_2(self):
        """
        The 22 is impossible
        """
        inputs = ['*120*112233#', 22, 2, 2, 2, 2, 2, 2]
        self.finish_input_sequence(inputs)

    def test_answer_wrong_type_1(self):
        inputs = ['*120*112233#', 1, "is there a 3rd option?", 1, 1, 1, 1, 2]
        self.finish_input_sequence(inputs)

    def test_answer_wrong_type_2(self):
        inputs = ['*120*112233#', 1, '*120*11223344#', 1, 1, 1, 2, 2]
        self.finish_input_sequence(inputs)

    def test_answer_wrong_type_3(self):
        """
        Mismatches on Continue question auto continue
        """
        inputs = ['*120*112233#', 1, 1, "huh", 1, 2, 1, 'exit', 1, 2]
        self.finish_input_sequence(inputs)

    def test_sequence_1(self):
        inputs = ['*120*112233#', 'blah', 55, '1', 1, 55, 55, 'blah',
                '1', 1, 55, 'blah', 55, '1', 2, 2, 2]
        self.finish_input_sequence(inputs)

    def test_sequence_2(self):
        """
        This sequence answers all questions,
        forcing an exit response to the final continure question
        """
        inputs = ['*120*112233#', 1, '1', 55, 2, '1', 55, 1, '1',
                55, 2, 55, '2', 1, '1', 1, '1', 1, 55, '2',
                55, '1', 1, '1', 55, '2', 'bye', 'no', 1, 2]
        self.finish_input_sequence(inputs)

    def test_sequence_3(self):
        inputs = ['*120*112233#', 'demo', '2', 55, 'demo', 55, 55, 55, 55,
                '2', 55, 'demo', '2', 55, 55, 'demo', '2', 1, 55,
                55, '1', 55, '2', 55, '2', 55, '2', 55, '2', 2]
        self.finish_input_sequence(inputs)

    def test_sequence_4(self):
        """
        Check the message on resuming after completion
        """
        inputs = ['*120*112233#', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        self.finish_input_sequence(inputs)
        self.session_event = 'new'
        inputs = ['*120*112233#']
        self.completed_input_sequence(inputs)

    def test_with_forced_order(self):
        force_order = [
            'demographic1',
            'demographic2',
            'demographic3',
            'demographic4',
            'question7',
            'continue',
            'question9',
            'continue',
            'question10',
            'continue',
            'question2',
            'continue',
            'question6',
            'continue',
            'question1',
            'continue',
            'question4',
            'continue',
            'question5',
            'continue',
            'question3',
            'continue',
            'question8',
            'continue'
            ]
        inputs = ['*120*112233#']
        resp = self.run_input_sequence(inputs, force_order)
        self.assertEqual(4, len(resp.option_list))
        inputs = [1]
        resp = self.run_input_sequence(inputs)
        self.assertEqual(2, len(resp.option_list))
        inputs = [1]
        resp = self.run_input_sequence(inputs)
        self.assertEqual(9, len(resp.option_list))
        inputs = [1]
        resp = self.run_input_sequence(inputs)
        self.assertEqual(4, len(resp.option_list))

        # simulate resume
        inputs = ['*120*112233#']
        self.session_event = 'new'
        resp = self.run_input_sequence(inputs)
        self.assertEqual(4, len(resp.option_list))

        # and 3 more attempts
        inputs = [1]
        self.session_event = 'new'
        resp = self.run_input_sequence(inputs)
        self.assertEqual(2, len(resp.option_list))
        inputs = [1]
        self.session_event = 'new'
        resp = self.run_input_sequence(inputs)
        self.assertEqual(2, len(resp.option_list))
        inputs = [None]
        self.session_event = 'new'
        resp = self.run_input_sequence(inputs)
        self.assertEqual(0, len(resp.option_list))


class FakeRedisInputSequenceTest(RedisInputSequenceTest):

    def tearDown(self):
        pass

    def set_ds(self):
        self.ds = FakeRedis()
