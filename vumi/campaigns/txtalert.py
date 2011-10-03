from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import task
from twisted.python import log
from vumi.service import Worker
from vumi.message import Message
from vumi.utils import safe_routing_key
from vumi.utils import http_request
from vumi.workers.integrat.worker import IntegratWorker
from datetime import datetime, timedelta, date
from urllib import urlopen, urlencode
import json
import base64

PATIENT_API_URL = 'http://qa.txtalert.praekeltfoundation.org/' \
                  'api/v1/patient.json'
REQUEST_CHANGE_URL = 'http://qa.txtalert.praekeltfoundation.org/' \
                    'api/v1/request/change.json'
REQUEST_CALL_URL = 'http://qa.txtalert.praekeltfoundation.org/' \
                    'api/v1/request/call.json'


def basic_auth_string(username, password):
    """
    Encode a username and password for use in an HTTP Basic Authentication
    header
    """
    b64 = base64.encodestring('%s:%s' % (username, password)).strip()
    return 'Basic %s' % b64


def get_patient(msisdn, patient_id):
    """get the patient info from txtalert & parse the JSON response"""
    url = '%s?%s' % (
        PATIENT_API_URL,
        urlencode({'msisdn': msisdn, 'patient_id': patient_id}))
    return json.loads(urlopen(url).read())


@inlineCallbacks
def request_change(visit_id, change_type, headers):
    default_headers = {
        'Content-Type': ['application/x-www-form-urlencoded'],
    }
    default_headers.update(headers)

    response = yield http_request(REQUEST_CHANGE_URL, urlencode({
        'visit_id': visit_id,
        'when': change_type,
    }), headers=default_headers, method='POST')
    print 'response', response
    returnValue(response)


def reschedule_earlier_date(visit_id, headers):
    log.msg("Rescheduling earlier date", visit_id)
    request_change(visit_id, 'earlier', headers)


def reschedule_later_date(visit_id, headers):
    log.msg("Rescheduling later date", visit_id)
    request_change(visit_id, 'later', headers)


@inlineCallbacks
def call_request(msisdn, headers):
    default_headers = {
        'Content-Type': ['application/x-www-form-urlencoded'],
    }
    default_headers.update(headers)

    response = yield http_request(REQUEST_CALL_URL, urlencode({
        'msisdn': msisdn,
    }), headers=default_headers, method='POST')
    print 'response', response
    returnValue(response)


class RestartConversationException(Exception):
    pass


class Menu(object):
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.finished = False
        self.msisdn = None

    def ask(self, q):
        return q

    def choice(self, q, choices):
        choice_string = "\n".join(["%s: %s" % args for args in choices])
        return self.ask("%s\n%s" % (q, choice_string))

    def close(self, s):
        self.finished = True
        return s

    def log(self, msg):
        log.msg("[%s] %s" % (getattr(self, 'session_id', 'unknown'), msg))

    def run(self):
        # we're expecting the client to initiate
        self.msisdn = yield
        self.log('Starting new session for %s' % self.msisdn)
        patient_id = yield self.ask('Welcome to txtAlert. '
                                    'Please respond with your patient id.')
        self.log('patient_id: %s' % patient_id)
        patient = get_patient(self.msisdn, patient_id)
        self.log('patient: %s' % patient)

        # check for a known patient
        if not patient:
            yield self.close('Sorry, cannot find your records. '
                             'Please contact your clinic.')

        # check for an appointment
        next_appointment_triplet = patient.get('next_appointment')
        if next_appointment_triplet == []:
            yield self.close('Sorry, there are no appointment scheduled. '
                             'Contact your clinic if you believe this '
                             'to be wrong')

        # construct the date from the JSON triplet
        next_date = date(*next_appointment_triplet)
        clinic = patient.get('clinic')

        # pop the questions
        answer = yield self.choice(
            'Welcome to txtAlert. '
            'Your next visit is on %s at %s' % (next_date, clinic),
            choices=(
                ('1', 'Change next appointment'),
                ('2', 'See attendance barometer'),
                ('3', 'Request call from clinic'),
            ))
        self.log('first choice: %s' % answer)

        if answer == '1':
            answer = yield self.choice(
                'Welcome to txtAlert. '
                'Appointment %s at %s' % (next_date, clinic),
                choices=(
                    ('1', 'Request earlier date'),
                    ('2', 'Request later date'),
                )
            )
            if answer == '1':
                reschedule_earlier_date(patient.get('visit_id'), {
                    'Authorization': [basic_auth_string(self.username,
                                                        self.password)],
                })
                answer = yield self.choice("Thanks! Your change request has "
                                "been registered. You'll receive an SMS with"
                                 " your appointment information", choices=(
                                    ('0', 'Return to main menu.'),
                                    ('1', 'Exit.'),
                                 ))
                if answer == '0':
                    raise RestartConversationException
                else:
                    yield self.close('Thank you and good bye!')

            elif answer == '2':
                reschedule_later_date(patient.get('visit_id'), {
                    'Authorization': [basic_auth_string(self.username,
                                                        self.password)],
                })
                answer = yield self.choice("Thanks! Your change request has "
                                "been registered. You'll receive an SMS with"
                                 " your appointment information", choices=(
                                    ('0', 'Return to main menu.'),
                                    ('1', 'Exit.'),
                                 ))
                if answer == '0':
                    raise RestartConversationException
                else:
                    yield self.close('Thank you and good bye!')
        elif answer == '2':
            answer = yield self.choice("Welcome to txtAlert.\n"
                        "You have attended %s%% of your appointments. \n"
                        "%s appointment(s) have been rescheduled.\n"
                        "%s appointment(s) missed.\n" % (
                            patient.get('attendance', 0),
                            patient.get('rescheduled', 0),
                            patient.get('missed', 0)), choices=(
                                ('0', 'Return to main menu.'),
                                ('1', 'Exit.'),
                            ))
            if answer == '0':
                raise RestartConversationException
            else:
                yield self.close('Thank you and good bye!')

        elif answer == '3':
            call_request(patient.get('msisdn'), {
                'Authorization': [basic_auth_string(self.username,
                                                    self.password)],
            })
            answer = yield self.choice("Welcome to txtAlert. "
                        "You have requested a call from the clinic. "
                        "You will be called as soon as possible.", choices=(
                            ('0', 'Return to main menu.'),
                            ('1', 'Exit.'),
                        ))
            if answer == '0':
                raise RestartConversationException
            else:
                yield self.close('Thank you and good bye!')

        # always close with this when this is reached, under normal conditions
        # it should never get here.
        yield self.close('Sorry, that wasn\'t what I expected. '
                         'Please reconnect and try again')


class BookingTool(Worker):

    TIMEOUT_PERIOD = timedelta(minutes=1)

    @inlineCallbacks
    def startWorker(self):
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s'
                                                % self.config['username'])
        self.consume('xmpp.inbound.gtalk.%s' % self.config['username'],
                     self.consume_message)
        self.sessions = {}

        self.gc = task.LoopingCall(self.garbage_collect)
        self.gc.start(1)

    def garbage_collect(self):
        for uuid, (timestamp, coroutine) in self.sessions.items():
            if timestamp < datetime.utcnow() - self.TIMEOUT_PERIOD:
                self.end_session(uuid)

    def session_exists(self, uuid):
        return uuid in self.sessions

    def serialize(self, obj):
        # return picklegenerators.dumps(obj)
        return (datetime.utcnow(), obj)

    def deserialize(self, obj):
        # menu, gen = picklegenerators.loads(obj)
        # gen.next()
        # return (menu, gen)
        return obj

    def end_session(self, uuid):
        log.msg('Ending session %s' % uuid)
        return self.sessions.pop(uuid, None)

    def save_session(self, uuid, menu, coroutine):
        self.sessions[uuid] = self.serialize((menu, coroutine))

    def get_session(self, uuid):
        dt, session = self.deserialize(self.sessions[uuid])
        return session

    def start_new_session(self, uuid, message):
        # assume the first message is the announcement
        menu = Menu(**self.config['txtalert'])
        coroutine = menu.run()
        coroutine.next()
        self.save_session(uuid, menu, coroutine)
        start_of_conversation = coroutine.send(message)  # announcement
        return start_of_conversation

    def resume_existing_session(self, uuid, message):
        # grab from memory
        menu, coroutine = self.get_session(uuid)
        try:
            response = coroutine.send(message)
            if menu.finished:
                menu, coroutine = self.end_session(uuid)
            else:
                self.save_session(uuid, menu, coroutine)
            return response
        except RestartConversationException:
            return self.start_new_session(uuid, menu.msisdn)

    def consume_message(self, message):
        log.msg('Active sessions:', self.sessions.keys())
        sender = message.payload['sender']
        message = message.payload['message']
        # it's a new session
        if self.session_exists(sender):
            response = self.resume_existing_session(sender, message)
        else:
            response = self.start_new_session(sender, message)
        log.msg('sending response: %s' % response)
        self.publisher.publish_message(Message(recipient=sender,
                                               message=response))

    @inlineCallbacks
    def stopWorker(self):
        yield None


class USSDBookingTool(IntegratWorker):
    @inlineCallbacks
    def startWorker(self):
        self.publisher = yield self.publish_to('ussd.outbound.%s' %
                                               self.config['transport_name'])
        self.sessions = {}
        self.consume('ussd.inbound.%s.%s' % (
            self.config['transport_name'],
            safe_routing_key(self.config['ussd_code']),
        ), self.consume_message)

    def session_exists(self, uuid):
        return uuid in self.sessions

    def serialize(self, obj):
        # return picklegenerators.dumps(obj)
        return (datetime.utcnow(), obj)

    def deserialize(self, obj):
        # menu, gen = picklegenerators.loads(obj)
        # gen.next()
        # return (menu, gen)
        return obj

    def end_session(self, uuid):
        log.msg('Ending session %s' % uuid)
        return self.sessions.pop(uuid, None)

    def save_session(self, uuid, menu, coroutine):
        self.sessions[uuid] = self.serialize((menu, coroutine))

    def get_session(self, uuid):
        dt, session = self.deserialize(self.sessions[uuid])
        return session

    def new_session(self, data):
        session_id = data['transport_session_id']
        msisdn = data['sender']

        menu = Menu(**self.config['txtalert'])
        coroutine = menu.run()
        coroutine.next()
        self.save_session(session_id, menu, coroutine)
        start_of_conversation = coroutine.send(msisdn)  # announcement
        log.msg('replying with', start_of_conversation)
        self.reply(session_id, start_of_conversation)

    def resume_session(self, data):
        session_id = data['transport_session_id']
        message = data['message']

        menu, coroutine = self.get_session(session_id)
        try:
            response = coroutine.send(message)

            if menu.finished:
                self.end(session_id, response)
            else:
                self.save_session(session_id, menu, coroutine)
                self.reply(session_id, response)
        except RestartConversationException:
            return self.new_session(data)

    def open_session(self, data):
        pass

    def close_session(self, data):
        session_id = data['transport_session_id']
        self.end_session(session_id)
