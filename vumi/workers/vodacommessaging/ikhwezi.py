
from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from vumi.message import Message
from vumi.service import Worker
from vumi.workers.vodacommessaging.utils import VodacomMessagingResponse


class IkhweziQuiz():

    LANGUAGE = {
            "English": "1",
            "Zulu": "2",
            "Afrikaans": "3",
            "Sotho": "4",
            "1": "English",
            "2": "Zulu",
            "3": "Afrikaans",
            "4": "Sotho"
            }

    QUIZ = {
            "question1": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question2": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question3": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question4": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question5": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question6": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question7": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question8": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question9": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "question10": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "demographic1": {
                "English": {
                    "headertext": "Thnx 4 taking the Quiz! Answer easy questions and be 1 of 5000 lucky winners.  Pick your language:",
                    "options": [
                        "English",
                        "Zulu",
                        "Afrikaans",
                        "Sotho"
                        ]
                    }
                },
            "demographic2": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "demographic3": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                },
            "demographic4": {
                "English": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Zulu": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Afrikaans": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    },
                "Sotho": {
                    "headertext": "",
                    "options": [
                        {
                            "text": "",
                            "answer": ""
                            },
                        ]
                    }
                }
        }

    def __init__(self, msisdn, datastore=None):
        msisdn = str(msisdn)
        self.datastore = datastore
        self.retrieve_entrant(msisdn) or self.new_entrant(msisdn)

    def retrieve_entrant(self, msisdn):
        answers = self.datastore.get(msisdn)
        if answers:
            self.answers = answers
            return True
        return False

    def new_entrant(self, msisdn):
        self.answers = {
                'msisdn': None,
                'first_contact_timestamp': None,
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
                'question1_timestamp': None,
                'question2_timestamp': None,
                'question3_timestamp': None,
                'question4_timestamp': None,
                'question5_timestamp': None,
                'question6_timestamp': None,
                'question7_timestamp': None,
                'question8_timestamp': None,
                'question9_timestamp': None,
                'question10_timestamp': None,
                'demographic1': None,
                'demographic2': None,
                'demographic3': None,
                'demographic4': None,
                'demographic1_timestamp': None,
                'demographic2_timestamp': None,
                'demographic3_timestamp': None,
                'demographic4_timestamp': None}
        self.datastore[msisdn] = self.answers
        return True

    def select_contextual_question(self):
        demographic1_answer = self.answers.get('demographic1')
        ########### testing #############
        demographic1_answer = self.LANGUAGE['English']
        import random
        ########### testing #############
        if not demographic1_answer:
            return 'demographic1', self.QUIZ.get('demographic1', {}).get('English')
        else:
            demo = 0
            for key, val in self.answers.items():
                if val and self.QUIZ.get(key):
                    if key.startswith('demo'):
                        demo += 1
                    else:
                        demo -= 1
                    print demo
            prefix = 'quest'
            if demo < 1:
                prefix = 'demo'

            question_list = []
            for key, val in self.answers.items():
                if val == None and self.QUIZ.get(key) and key.startswith(
                        prefix):
                    question_list.append({
                        'context': key,
                        'question': self.QUIZ.get(key)[self.LANGUAGE.get(
                            demographic1_answer)]})
            choice = random.choice(question_list)
            return choice['context'], choice['question']

config = {
        'web_host': 'vumi.p.org',
        'web_path': '/api/v1/ussd/vmes/'}
ds = {}
ik = IkhweziQuiz('111', ds)
ik = IkhweziQuiz('111', ds)
ik = IkhweziQuiz('211', ds)
context, question = ik.select_contextual_question()
vmr = VodacomMessagingResponse(config)
vmr.set_context(context)
vmr.set_headertext(question['headertext'])
for option in question['options']:
    vmr.add_option(option['text'])
print vmr

for msisdn,ans in ds.items():
    print msisdn, ans


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
