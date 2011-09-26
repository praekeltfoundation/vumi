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

max_cycles = [90]

xml_samples = {}

print ""
print "################################################"
print "################################################"
print "################################################"
print "################################################"
print ""

def respond(context, answer):
    print max_cycles
    print context, "--> ",  answer
    ik = IkhweziQuiz(config, quiz, translations, ds, '08212345670')
    resp = ik.formulate_response(context, answer)
    print "\n", resp
    xml_samples[str(resp)] = True
    context = resp.context
    answer = None
    if len(resp.option_list):
        answer = str(random.choice(resp.option_list)['order'])
        if context == "demographic1":
            answer = 1
        if context == "continue":
            answer = random.choice([1, 1, 2])
        max_cycles[0] -= 1
        answer = random.choice([answer, answer, answer, None])
        if max_cycles[0]:
                respond(context, answer)


respond(None, None)

ik = IkhweziQuiz(config, quiz, translations, ds, '08212345670')

print ""
print "################################################"
print "################################################"
print "################################################"
print "################################################"
print ""

print ''
for k, v in ik.data.items():
    print "%s: %s" % (k, v)


