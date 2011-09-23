import yaml

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.vodacommessaging.ikhwezi import (
        QUIZ, IkhweziQuiz, IkhweziQuizWorker)

quiz = yaml.load(QUIZ)
config = {
        'web_host': 'vumi.p.org',
        'web_path': '/api/v1/ussd/vmes/'}
ds = {}
ik = IkhweziQuiz(quiz, '111', ds)
ik = IkhweziQuiz(quiz, '111', ds)
ik = IkhweziQuiz(quiz, '211', ds)

context, question = ik.select_contextual_question()
vmr = VodacomMessagingResponse(config)
vmr.set_context(context)
vmr.set_headertext(question['headertext'])
for key, val in question['options'].items():
    vmr.add_option(val['text'], key)
print vmr

ik = IkhweziQuiz(quiz, '211', ds)
print ik.answer_contextual_question("demographic1", "1")

ik = IkhweziQuiz(quiz, '211', ds)
print ik.answer_contextual_question("question1", "1")

for msisdn,ans in ds.items():
    print msisdn, ans

#print yaml.dump(IkhweziQuiz.quiz)
#print yaml.dump(IkhweziQuiz.language)
#print yaml.dump(ik.quiz)
