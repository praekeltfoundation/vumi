import yaml
import random

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.vodacommessaging.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker)


######### Dynamic Test generator #######
quiz = yaml.load(QUIZ)
translations = yaml.load(TRANSLATIONS)
config = {
        'web_host': 'vumi.p.org',
        'web_path': '/api/v1/ussd/vmes/'}
ds = {}
order = []
answers = []
max_cycles = [99]

def respond(context, answer):
    #print context, answer
    answers.append(answer)
    ik = IkhweziQuiz(config, quiz, translations, ds, '08212345670')
    if context == None:
        for o in ik.data['order']:
            order.append(o)
    #print ik.data['order']
    resp = ik.formulate_response(context, answer)
    #print ik.data['order']
    #print resp
    context = resp.context
    answer = None
    if len(resp.option_list):
        answer = str(random.choice(resp.option_list)['order'])
        if context == "continue":
            answer = random.choice([1, 1, 1, 2])
        max_cycles[0] -= 1
        answer = random.choice([answer, answer, answer, None])
        if max_cycles[0]:
                respond(context, answer)

respond(None, None)
#print order
#print answers
########################################


class InputSequenceTest(TestCase):

    def setUp(self):
        self.msisdn = '0821234567'
        self.quiz = yaml.load(QUIZ)
        self.translations = yaml.load(TRANSLATIONS)
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}
        self.ds = {}

    def get_quiz_entry(self):
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.ds,
                self.msisdn)
        return ik

    def exit_text(self):
        return quiz['exit']['headertext']

    def tearDown(self):
        pass

    def stripNoneInputs(self, inputs):
        outputs = []
        for i in inputs:
            if i != None:
                outputs.append(i)
        return outputs

    def finishInputSequence(self, inputs):
        resp = [None]  # haven't started yet
        context = [None]  # ditto
        for user_in in inputs:
            #print context[0]
            #print user_in
            if user_in == 'demo':
                user_in = 1 # no way to predict dempgraphic questions 
            ik = self.get_quiz_entry()
            resp[0] = ik.formulate_response(context[0], user_in)
            #print resp[0]
            #print ""
            #for k, v in ik.data.items():
                #if k.startswith('question'):
                    #print k, v
            context[0] = resp[0].context
        self.assertEquals(str(resp[0].headertext), self.exit_text())
        inputs = self.stripNoneInputs(inputs)
        self.assertEquals(str(resp[0].headertext), self.exit_text())

    def testSequence1(self):
        inputs = [None, 'demo', None, '1', 1, None, None, 'demo',
                '1', 1, None, 'demo', None, '1', 2]
        self.finishInputSequence(inputs)

    def testSequence2(self):
        """
        This sequence answers all questions,
        forcing an exit response to the final continure question
        """
        inputs = [None, 'demo', '1', None, 'demo', '1', None, 'demo', '1',
                None, 'demo', None, '2', 1, '1', 1, '1', 1, None, '2',
                None, '1', 1, '1', None, '2', 2]
        self.finishInputSequence(inputs)

    def testSequence2(self):
        """
        This sequence answers all questions,
        forcing an exit response to the final continure question
        """
        inputs = [None, 'demo', '2', None, 'demo', None, None, None, None,
                '2', None, 'demo', '2', None, None, 'demo', '2', 1, None,
                None, '1', None, '2', None, '2', None, '2', None, '2', 2]
        self.finishInputSequence(inputs)

