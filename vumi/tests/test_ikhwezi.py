import yaml
import random

from twisted.python import log
from twisted.trial.unittest import TestCase
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse
from vumi.workers.vodacommessaging.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker)

quiz = yaml.load(QUIZ)
translations = yaml.load(TRANSLATIONS)

config = {
        'web_host': 'vumi.p.org',
        'web_path': '/api/v1/ussd/vmes/'}

ds = {}

ik = IkhweziQuiz(config, quiz, translations, ds, '08212345670')

print "\n\n################################################\n\n"
def respond(context, answer):
    resp = ik.formulate_response(context, answer)
    print "\n", resp
    respond(resp.context, random.choice(resp.option_list)['order'])

respond(None, None)

#print ik.formulate_response(None, None)
#print ik.formulate_response("demographic1", "1")
#print ik.formulate_response("question1", "1")
#print ik.formulate_response("continue", "1")


#resp = ik.formulate_response(None, None)
#print resp.context
#print random.choice(resp.option_list)['order']

#for msisdn,ans in ds.items():
    #print msisdn, ans

