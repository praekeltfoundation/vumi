import uuid

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.resource import Resource
from twisted.web.server import NOT_DONE_YET
from vumi.message import Message
from vumi.service import Worker


class HttpRpcHealthResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_GET(self, request):
        request.setResponseCode(http.OK)
        return "pReq:%s" % len(self.transport.requests)


class HttpRpcResource(Resource):
    isLeaf = True

    def __init__(self, transport):
        self.transport = transport
        Resource.__init__(self)

    def render_(self, request, logmsg=None):
        request.setHeader("content-type", "text/plain")
        uu = str(uuid.uuid4())
        md = {}
        md['args'] = request.args
        md['content'] = request.content.read()
        md['path'] = request.path
        if logmsg:
            log.msg("HttpRpcResource", logmsg, "Message.message:", repr(md))
        message = Message(message=md, uuid=uu,
                          return_path=[self.transport.consume_key])
        self.transport.publisher.publish_message(message)
        self.transport.requests[uu] = request
        return NOT_DONE_YET

    def render_GET(self, request):
        return self.render_(request, "render_GET")

    def render_POST(self, request):
        return self.render_(request, "render_POST")


class HttpRpcTransport(Worker):

    @inlineCallbacks
    def startWorker(self):
        self.uuid = uuid.uuid4()
        log.msg("Starting HttpRpcTransport %s config: %s" % (self.uuid,
                                                             self.config))
        self.publish_key = self.config['publish_key']
        self.consume_key = self.config['consume_key']

        self.requests = {}

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (HttpRpcResource(self), self.config['web_path']),
                (HttpRpcHealthResource(self), 'health'),
            ],
            self.config['web_port'])
        print self.receipt_resource

    def consume_message(self, message):
        log.msg("HttpRpcTransport consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        if message.payload.get('uuid') and 'message' in message.payload:
            self.finishRequest(
                    message.payload['uuid'],
                    message.payload['message'])

    def finishRequest(self, uuid, message=''):
        data = str(message)
        log.msg("HttpRpcTransport.finishRequest with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.write(data)
            request.finish()
            del self.requests[uuid]

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")


class VodacomMessagingResponse(object):
    def __init__(self, config):
        self.config = config
        self.context = ''
        self.freetext_option = None
        self.template_freetext_option_string = '<option' \
            + ' command="1"' \
            + ' order="1"' \
            + ' callback="http://%(web_host)s%(web_path)s?context=%(context)s"' \
            + ' display="False"' \
            + ' ></option>'
        self.option_list = []
        self.template_numbered_option_string = '<option' \
            + ' command="%(order)s"' \
            + ' order="%(order)s"' \
            + ' callback="http://%(web_host)s%(web_path)s?context=%(context)s"' \
            + ' display="True"' \
            + ' >%(text)s</option>'

    def set_headertext(self, headertext):
        self.headertext = headertext

    def set_context(self, context):
        """
        context is a unique identifier for the state that generated
        the message the user is responding to
        """
        self.context = context
        if self.freetext_option:
            self.accept_freetext()
        count = 0
        while count < len(self.option_list):
            self.option_list[count].update({'context': self.context})
            count += 1

    def add_option(self, text):
        self.freetext_option = None
        dict = {'text': str(text)}
        dict['order'] = len(self.option_list) + 1
        dict.update({
            'web_path': self.config['web_path'],
            'web_host': self.config['web_host'],
            'context': self.context})
        self.option_list.append(dict)

    def accept_freetext(self):
        self.option_list = []
        self.freetext_option = self.template_freetext_option_string % {
            'web_path': self.config['web_path'],
            'web_host': self.config['web_host'],
            'context': self.context}

    def __str__(self):
        headertext = '\t<headertext>%s</headertext>\n' % self.headertext
        options = ''
        if self.freetext_option or len(self.option_list) > 0:
            options = '\t<options>\n'
            for o in self.option_list:
                options += '\t\t' + self.template_numbered_option_string % o + '\n'
            if self.freetext_option:
                options += '\t\t' + self.freetext_option + '\n'
            options += '\t</options>\n'
        response = '<request>\n' + headertext + options + '</request>'
        return response


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

    #some old code I might want
    def rowset(conn, sql, presql=[]):
        cursor = conn.cursor()
        for s in presql:
            #print s
            cursor.execute(s)
        #print sql
        cursor.execute(sql)
        result = cursor.fetchall()
        hashlist = []
        names = []
        for column in cursor.description:
            names.append(column[0])
        for row in result:
            hash = {}
            index = 0
            while index < len(names):
                hash[names[index]] = row[index]
                index+=1
            hashlist.append(hash)
        return hashlist

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


class DummyRpcWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting DummyRpcWorker with config: %s" % (self.config))
        self.publish_key = self.config['consume_key']  # Swap consume and
        self.consume_key = self.config['publish_key']  # publish keys

        self.publisher = yield self.publish_to(self.publish_key)
        self.consume(self.consume_key, self.consume_message)

    def consume_message(self, message):
        hi = '''
<request>
    <headertext>Welcome to Vodacom Ikhwezi!</headertext>
    <options>
        <option
            command="1"
            order="1"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">finish session.</option>
        <option
            command="2"
            order="2"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">return to this screen.</option>
        <option
            command="3"
            order="3"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">continue to next screen.</option>
    </options>
</request>'''
        cont = '''
<request>
    <headertext>Nothing to see yet</headertext>
    <options>
        <option
            command="1"
            order="1"
            callback="http://vumi.praekeltfoundation.org/api/v1/ussd/ikhwezi/"
            display="True">finish session.</option>
    </options>
</request>'''
        bye = '''
<request>
    <headertext>Goodbye!</headertext>
</request>'''

        log.msg("DummyRpcWorker consuming on %s: %s" % (
            self.consume_key,
            repr(message.payload)))
        reply = hi
        if message.payload['message']['args'].get('request', [''])[0] == "1":
            reply = bye
        if message.payload['message']['args'].get('request', [''])[0] == "3":
            reply = cont
        self.publisher.publish_message(Message(
                uuid=message.payload['uuid'],
                message=reply),
            routing_key=message.payload['return_path'].pop())

    def stopWorker(self):
        log.msg("Stopping the MenuWorker")
