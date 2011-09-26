import random

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from vumi.message import Message
from vumi.service import Worker
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

    def exit_suffix(self, context=None, answer=None):
        ans_lists = self.answered_lists()
        if len(ans_lists['q_un']) == 0 or (
                context == 'continue' and int(answer) == 2):
            context = 'exit'
            text = self.quiz.get(context).get('headertext')
            return text
        return False

    def next_context_and_question(self):
        context = None
        question = None
        if not self.answers.get('demographic1'):
            context = 'demographic1'
            question = self.quiz.get(context)
        else:
            ans_lists = self.answered_lists()
            if len(ans_lists['d_ans']) > len(ans_lists['q_ans']) \
                    or len(ans_lists['d_un']) == 0:
                if self.exit_suffix(ans_lists):
                    context = 'exit'
                    question = self.quiz.get(context)
                else:
                    context = random.choice(ans_lists['q_un'])
            else:
                context = random.choice(ans_lists['d_un'])
            question = self.quiz.get(context)
        return context, question

    def answer_contextual_question(self, context, answer):
        answer = int(answer)
        question = self.quiz.get(context)
        if context in self.answers.keys():
            self.answers[context] = answer
        reply = question['options'][answer].get('reply')
        return reply

    def formulate_response(self, context, answer):
        reply = None

        if context and answer:
            reply = self.answer_contextual_question(context, answer)
        exit = self.exit_suffix(context, answer)

        if exit:
            context = 'exit'
            replyexit = reply or '' + exit
            vmr = VodacomMessagingResponse(self.config)
            vmr.set_context(context)
            vmr.set_headertext(replyexit)
            return vmr

        elif reply:
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
