import random

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from vumi.message import Message
from vumi.service import Worker
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse


TRANSLATIONS = '''
'''

QUIZ = '''
continue:
    options:
        1:
            text: "Continue to win!"
        2:
            text: "Exit the quiz"

demographic1:
    headertext: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000
                lucky winners.  Pick your language:"
    options:
        1:
            text: "English"
        2:
            text: "Zulu"
        3:
            text: "Afrikaans"
        4:
            text: "Sotho"

demographic2:
    headertext: "header"
    options:
        1:
            text: "que 1"
        2:
            text: "que 2"

demographic3:
    headertext: "header"
    options:
        1:
            text: "que 1"
        2:
            text: "que 2"

demographic4:
    headertext: "header"
    options:
        1:
            text: "que 1"
        2:
            text: "que 2"

question1:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question2:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question3:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question4:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question5:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question6:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question7:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question8:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question9:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"

question10:
    headertext: "header"
    options:
        1:
            text: "que 1"
            reply: "reply 1"
        2:
            text: "que 2"
            reply: "reply 2"
'''


class IkhweziQuiz():

    def __init__(self, config, quiz, translations, datastore, msisdn):
        self.config = config
        self.quiz = quiz
        self.translations = translations
        self.datastore = datastore
        msisdn = str(msisdn)
        self.retrieve_entrant(msisdn) or self.new_entrant(msisdn)

    def lang(self, lang):
        langs = {
                "English": "1",
                "Zulu": "2",
                "Afrikaans": "3",
                "Sotho": "4",
                "1": "English",
                "2": "Zulu",
                "3": "Afrikaans",
                "4": "Sotho"
                }
        return langs.get(str(lang))

    def retrieve_entrant(self, msisdn):
        answers = self.datastore.get(msisdn)
        if answers:
            self.answers = answers
            self.language = self.lang(answers['demographic1'])
            return True
        return False

    def new_entrant(self, msisdn):
        self.language = "English"
        self.answers = {
                'msisdn': str(msisdn),
                'm_timestamp': None,
                'question1': None,
                'question2': None,
                'question3': None,
                'question4': None,
                'question5': None,
                'question6': None,
                'question7': None,
                'question8': None,
                'question9': None,
                'question10': None,
                'q1_timestamp': None,
                'q2_timestamp': None,
                'q3_timestamp': None,
                'q4_timestamp': None,
                'q5_timestamp': None,
                'q6_timestamp': None,
                'q7_timestamp': None,
                'q8_timestamp': None,
                'q9_timestamp': None,
                'q10_timestamp': None,
                'demographic1': None,
                'demographic2': None,
                'demographic3': None,
                'demographic4': None,
                'd1_timestamp': None,
                'd2_timestamp': None,
                'd3_timestamp': None,
                'd4_timestamp': None}
        self.datastore[msisdn] = self.answers
        return True

    def answered_lists(self):
        q_un = []
        q_ans = []
        d_un = []
        d_ans = []
        for key, val in self.answers.items():
            if key.startswith('question') and val == None:
                q_un.append(key)
            if key.startswith('question') and val != None:
                q_ans.append(key)
            if key.startswith('demographic') and val == None:
                d_un.append(key)
            if key.startswith('demographic') and val != None:
                d_ans.append(key)
        return {'q_un': q_un, 'q_ans': q_ans, 'd_un': d_un, 'd_ans': d_ans}

    def next_context_and_question(self):
        context = None
        question = None
        if not self.answers.get('demographic1'):
            context = 'demographic1'
            question = self.quiz.get(context)
        else:
            ans_lists = self.answered_lists()
            #print ans_lists
            if len(ans_lists['d_ans']) > len(ans_lists['q_ans']):
                context = random.choice(ans_lists['q_un'])
            else:
                context = random.choice(ans_lists['d_un'])
            question = self.quiz.get(context)
        #print context, question
        return context, question

    def answer_contextual_question(self, context, answer):
        answer = int(answer)
        question = self.quiz.get(context)
        self.answers[context] = answer
        #import yaml
        #print yaml.dump({self.answers['msisdn']: self.answers.items()})
        reply = question['options'][answer].get('reply')
        return reply

    def formulate_response(self, context, answer):
        print context, answer
        reply = None
        if context and answer:
            reply = self.answer_contextual_question(context, answer)
        if reply:
            context = 'continue'
            question = self.quiz.get(context)
            vmr = VodacomMessagingResponse(self.config)
            vmr.set_context(context)
            vmr.set_headertext(reply)
            for key, val in question['options'].items():
                vmr.add_option(val['text'], key)
            return vmr
        else:
            context, question = self.next_context_and_question()
            vmr = VodacomMessagingResponse(self.config)
            vmr.set_context(context)
            vmr.set_headertext(question['headertext'])
            for key, val in question['options'].items():
                vmr.add_option(val['text'], key)
            return vmr





class IkhweziQuizWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting IkhweziQuizWorker with config: %s" % (self.config))
        self.publish_key = self.config['consume_key']  # Swap consume and
        self.consume_key = self.config['publish_key']  # publish keys

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

    def consume_message(self, message):
        log.msg("IkhweziQuizWorker consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        reply = "inspect message and create reply"  #TODO
        self.publisher.publish_message(Message(
                uuid=message.payload['uuid'],
                message=reply),
            routing_key=message.payload['return_path'].pop())

    def stopWorker(self):
        log.msg("Stopping the MenuWorker")
