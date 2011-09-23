import random

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from vumi.message import Message
from vumi.service import Worker


QUIZ = '''
continue:
  English:
    options:
      1:
        text: Continue
      2:
        text: Finish
demographic1:
  English:
    headertext: 'Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky
      winners.  Pick your language:'
    options:
      1:
        text: English
      2:
        text: Zulu
      3:
        text: Afrikaans
      4:
        text: Sotho
demographic2:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
demographic3:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
demographic4:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question1:
  Afrikaans:
    headertext: ''
    options:
        1:
            text: ''
            reply: '22222222'
  English:
    headertext: ''
    options:
        1:
            text: ''
            reply: '11111111'
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question10:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question2:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question3:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question4:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question5:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question6:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question7:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question8:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
question9:
  Afrikaans:
    headertext: ''
    options:
    - {answer: '', text: ''}
  English:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Sotho:
    headertext: ''
    options:
    - {answer: '', text: ''}
  Zulu:
    headertext: ''
    options:
    - {answer: '', text: ''}
'''


class IkhweziQuiz():

    def __init__(self, quiz, msisdn, datastore=None):
        self.quiz = quiz
        msisdn = str(msisdn)
        self.datastore = datastore
        self.retrieve_entrant(msisdn) or self.new_entrant(msisdn)
        self.language = {
                "English": "1",
                "Zulu": "2",
                "Afrikaans": "3",
                "Sotho": "4",
                "1": "English",
                "2": "Zulu",
                "3": "Afrikaans",
                "4": "Sotho"
                }

    def retrieve_entrant(self, msisdn):
        answers = self.datastore.get(msisdn)
        if answers:
            self.answers = answers
            return True
        return False

    def new_entrant(self, msisdn):
        self.answers = {
                'msisdn': None,
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

    def select_contextual_question(self):
        context = None
        question = None
        demographic1_answer = self.answers.get('demographic1')
        if not demographic1_answer:
            context = 'demographic1'
            question = self.quiz.get(context)['English']
        else:
            ans_lists = self.answered_lists()
            print ans_lists
            if len(ans_lists['d_ans']) > len(ans_lists['q_ans']):
                context = random.choice(ans_lists['q_un'])
            else:
                context = random.choice(ans_lists['d_un'])
            question = self.quiz.get(context)[self.language.get(str(
                            demographic1_answer))]
        print context, question
        return context, question

    def answer_contextual_question(self, context, answer):
        answer = int(answer)
        if context == 'demographic1':
            demographic1_answer = answer
        else:
            demographic1_answer = self.answers.get('demographic1')
        question = self.quiz.get(context)[self.language.get(str(
            demographic1_answer))]
        self.answers[context] = answer
        return question['options'][answer].get('reply')


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
