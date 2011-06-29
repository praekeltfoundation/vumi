from twisted.internet.defer import inlineCallbacks
from generator_tools import picklegenerators
from twisted.internet import task
from twisted.python import log
from vumi.service import Worker
from vumi.message import Message
from datetime import datetime, timedelta, date
from urllib import urlopen, urlencode
import json

PATIENT_API_URL = 'http://qa.txtalert.praekeltfoundation.org/' \
                  'api/v1/patient.json'


def get_patient(msisdn, patient_id):
    """get the patient info from txtalert & parse the JSON response"""
    url = '%s?%s' % (
        PATIENT_API_URL,
        urlencode({'msisdn': msisdn, 'patient_id': patient_id}))
    return json.loads(urlopen(url).read())


def reschedule_earlier_date(*args, **kwargs):
    log.msg("Rescheduling earlier date", args, kwargs)


def reschedule_later_date(*args, **kwargs):
    log.msg("Rescheduling later date", args, kwargs)


class Menu(object):
    def __init__(self):
        self.finished = False

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
        msisdn = yield
        self.log('Starting new session for %s' % msisdn)
        patient_id = yield self.ask('Welcome to txtAlert. '
                                    'Please respond with your patient id.')
        self.log('patient_id: %s' % patient_id)
        patient = get_patient(msisdn, patient_id)
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
                reschedule_earlier_date()
                yield self.close("Thanks! Your change request has been"
                                 " registered. You'll receive an SMS with"
                                 " your appointment information")
            elif answer == '2':
                reschedule_later_date()
                yield self.close("Thanks! Your change request has been"
                                 " registered. You'll receive an SMS with"
                                 " your appointment information")
        elif answer == '2':
            yield self.close("Welcome to txtAlert.\n"
                        "You have attended %s%% of your appointments. \n"
                        "%s appointments have been rescheduled.\n"
                        "%s appointments missed.\n" % (
                            patient.get('attendance', 0),
                            patient.get('rescheduled', 0),
                            patient.get('missed', 0)))
        elif answer == '3':
            yield self.close("Welcome to txtAlert. "
                        "You have requested a call from the clinic. "
                        "You will be called as soon as possible.")

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
        menu = Menu()
        coroutine = menu.run()
        coroutine.next()
        self.save_session(uuid, menu, coroutine)
        start_of_conversation = coroutine.send(message)  # announcement
        return start_of_conversation

    def resume_existing_session(self, uuid, message):
        # grab from memory
        menu, coroutine = self.get_session(uuid)
        response = coroutine.send(message)
        if menu.finished:
            menu, coroutine = self.end_session(uuid)
        else:
            self.save_session(uuid, menu, coroutine)
        return response

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
