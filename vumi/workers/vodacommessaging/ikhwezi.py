import random
import json
import yaml

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from vumi.message import Message, TransportUserMessage
from vumi.service import Worker
from vumi.tests.utils import FakeRedis
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse


TRANSLATIONS = '''
'''

QUIZ = '''
winner:
    headertext: "Thnx 4 taking the Quiz.  U have won R12 airtime! We will send U your airtime voucher. For more info about HIV/AIDS pls phone Aids Helpline 0800012322"

nonwinner:
    headertext: "Thnx 4 taking the HIV Quiz.  Unfortunately U are not a lucky winner. For more info about HIV/AIDS, phone Aids Helpline 0800012322"

continue:
    options:
        1:
            text: "Continue to win!"
        2:
            text: "Exit the quiz"

exit:
    headertext: "Thnx for taking the quiz.  We'll let U know if U R a lucky winner within 48 hours."

completed:
    headertext: "Thank you, you've completed the HIV quiz and will be notified via SMS if you've won airtime prize."

demographic1:
    headertext: "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:"
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
    headertext: "Please pick your gender"
    options:
        1:
            text: "Male"
        2:
            text: "Female"

demographic3:
    headertext: "Please pick your province"
    options:
        1:
            text: "Gauteng"
        2:
            text: "Kwazulu Natal"
        3:
            text: "Limpopo"
        4:
            text: "Eastern Cape"
        5:
            text: "Mpumalanga"
        6:
            text: "Northern Cape"
        7:
            text: "Western Cape"
        8:
            text: "Free State"
        9:
            text: "North West"

demographic4:
    headertext: "Please pick your age group"
    options:
        1:
            text: "younger than 20"
        2:
            text: "between 20-29"
        3:
            text: "between 30-39"
        4:
            text: "40 or older"

question1:
    headertext: "Can U get HIV/AIDS by having sex without a condom?"
    options:
        1:
            text: "Yes"
            reply: "Correct, U can get HIV/AIDS by having sex without a condom."
        2:
            text: "No"
            reply: "Please note: U can get HIV/AIDS by having sex without a condom."

question2:
    headertext: "Can traditional medicine cure HIV/AIDS?"
    options:
        1:
            text: "Yes"
            reply: "Please note: Traditional medicine cannot cure HIV/AIDS. There is no cure for HIV/AIDS."
        2:
            text: "No"
            reply: "Correct, traditional medicine can not cure HIV/AIDS.  There is no cure for HIV/AIDS."

question3:
    headertext: "Is an HIV test at any government clinic free of charge?"
    options:
        1:
            text: "Yes"
            reply: "Correct, an HV test at any government clinic is free of charge."
        2:
            text: "No"
            reply: "Please Note: an HIV test at any government clinic is free of charge."

question4:
    headertext: "Is it possible for a person newly infected with HIV, to test HIV negative?"
    options:
        1:
            text: "Yes"
            reply: "Correct, a newly-infected HIV positive person can test HIV-negative.  This is due to a person testing for HIV in the 'window period'"
        2:
            text: "No"
            reply: "Please Note: a newly-infected HIV positive person can test HIV-negative.  This is due to a person testing for HIV in the 'window period'"

question5:
    headertext: "Can HIV be transmitted by sweat?"
    options:
        1:
            text: "Yes"
            reply: "Please note, HIV can not be transmitted by sweat."
        2:
            text: "No"
            reply: "Correct, HIV can not be transmitted by sweat."

question6:
    headertext: "Is there a herbal medication that can cure HIV/AIDS?"
    options:
        1:
            text: "Yes"
            reply: "Please note, there are no herbal medications that can cure HIV/AIDS.  There is no cure for HIV/AIDS."
        2:
            text: "No"
            reply: "Correct, there are no herbal medications that can cure HIV/AIDS.  There is no cure for HIV/AIDS."

question7:
    headertext: "Does a CD4-count tell the strength of a person's immune system?"
    options:
        1:
            text: "Yes"
            reply: "Correct, a CD4-count does tell the strength of a person's immune system."
        2:
            text: "No"
            reply: "Please note, a CD4-count does tell the strength of a person's immune system."

question8:
    headertext: "Can HIV be transmitted through a mother's breast milk?"
    options:
        1:
            text: "Yes"
            reply: "Correct, HIV can be transmitted via a mother's breast milk."
        2:
            text: "No"
            reply: "Please note, HIV can be transmitted via a mother's breast milk."

question9:
    headertext: "Is it possible for an HIV positive woman to deliver an HIV negative baby?"
    options:
        1:
            text: "Yes"
            reply: "Correct, with treatment and planning, it is possible for an HIV positive mother to deliver an HIV-negative baby."
        2:
            text: "No"
            reply: "Please note, with treatment and planning, it is possible for an HIV positive mother to deliver an HIV-negative baby."

question10:
    headertext: "Do you immediately have to start ARVs when you test HIV positive?"
    options:
        1:
            text: "Yes"
            reply: "Please note, once testing positive for HIV, you first have to get your CD4 test done, to determine if you qualify for ARVs."
        2:
            text: "No"
            reply: "Correct, once testing positive for HIV, you first have to get your CD4 test done, to determine if you qualify for ARVs.
1. Continue to win!"
'''


class IkhweziQuiz():

    REDIS_PREFIX = "vumi_vodacom_ikhwezi"

    def __init__(self, config, quiz, translations, datastore, msisdn):
        self.config = config
        self.quiz = quiz
        self.translations = translations
        self.datastore = datastore
        msisdn = str(msisdn)
        self.retrieve_entrant(msisdn) or self.new_entrant(msisdn)

    def ds_set(self):
        self.datastore.set("%s#%s" % (
            self.REDIS_PREFIX, self.data['msisdn']), json.dumps(self.data))

    def ds_get(self, msisdn):
        data_string = self.datastore.get("%s#%s" % (
            self.REDIS_PREFIX, msisdn))
        if data_string:
            return json.loads(data_string)
        return None

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
        data = self.ds_get(msisdn)
        if data:
            self.data = data
            self.language = self.lang(data['demographic1'])
            return True
        return False

    def random_ordering(self):
        order = ['demographic1']
        demographics = [
                'demographic2',
                'demographic3',
                'demographic4',
                ]
        random.shuffle(demographics)
        questions = [
                'question1',
                'question2',
                'question3',
                'question4',
                'question5',
                'question6',
                'question7',
                'question8',
                'question9',
                'question10']
        random.shuffle(questions)
        while len(questions):
            order.append(questions.pop())
            if len(demographics):
                order.append(demographics.pop())
        return order

    def force_order(self, order):
        self.data['order'] = order
        self.ds_set()

    def new_entrant(self, msisdn):
        self.language = "English"
        self.data = {
                'msisdn': str(msisdn),
                'attempts': 0,
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
                'd4_timestamp': None,
                'order': self.random_ordering()}
        self.ds_set()
        return True

    def next_context_and_question(self):
        context = None
        question = None
        order = self.data['order']
        if len(order):
            context = order[0]
            question = self.quiz.get(context)
        return context, question

    def answer_contextual_question(self, context, answer):
        reply = None
        question = self.quiz.get(context)
        if context in self.data.keys() \
                and answer in question['options'].keys() \
                and context in self.data['order']:
            reply = question['options'][answer].get('reply')
            self.data[context] = answer
            self.data['order'].pop(0)
            self.ds_set()
        return reply

    def formulate_response(self, context, answer):
        try:
            answer = int(answer)
        except Exception, e:
            log.msg(e)
            answer = None

        if context == None:  # new session
            self.data['attempts'] += 1
            self.ds_set()

        if self.data['attempts'] > 4:
            # terminate interaction
            context = 'completed'
            question = self.quiz.get(context)
            vmr = VodacomMessagingResponse(self.config)
            vmr.set_context(context)
            vmr.set_headertext(question['headertext'])
            return vmr

        reply = None
        if context and answer:
            reply = self.answer_contextual_question(context, answer)

        if (context == 'continue' or context == None) and (
                answer == 2 or len(self.data['order']) == 0):
            # don't continue, show exit
            context = 'exit'
            question = self.quiz.get(context)
            vmr = VodacomMessagingResponse(self.config)
            vmr.set_context(context)
            vmr.set_headertext(question['headertext'])
            return vmr

        #elif reply and len(self.data['order']) == 0:
            ## correct answer and exit
            #context = 'exit'
            #question = self.quiz.get(context)
            #vmr = VodacomMessagingResponse(self.config)
            #vmr.set_context(context)
            #vmr.set_headertext("%s  %s" % (reply, question['headertext']))
            #return vmr

        elif reply:
            # correct answer and offer to continue
            context = 'continue'
            question = self.quiz.get(context)
            vmr = VodacomMessagingResponse(self.config)
            vmr.set_context(context)
            vmr.set_headertext(reply)
            for key, val in question['options'].items():
                vmr.add_option(val['text'], key)
            return vmr

        else:
            # ask another question
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
        self.publish_key = self.config['publish_key']
        self.consume_key = self.config['consume_key']

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

        self.quiz = yaml.load(QUIZ)
        self.translations = yaml.load(TRANSLATIONS)
        self.ds = FakeRedis()

    def consume_message(self, message):
        log.msg("IkhweziQuizWorker consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        user_m = TransportUserMessage(**message.payload)
        request = user_m.payload['content']['args'].get('request', [None])[0]
        context = user_m.payload['content']['args'].get('context', [None])[0]
        msisdn = user_m.payload['content']['args'].get('msisdn', [None])[0]
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.ds,
                msisdn)
        resp = ik.formulate_response(context, request)
        reply = user_m.reply(str(resp))
        self.publisher.publish_message(reply)

    def stopWorker(self):
        log.msg("Stopping the IkhweziQuizWorker")
