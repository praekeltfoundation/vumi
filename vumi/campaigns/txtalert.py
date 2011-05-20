from twisted.internet.defer import inlineCallbacks, Deferred
from generator_tools import picklegenerators
from twisted.internet import task
from twisted.python import log
from twisted.protocols import basic
from vumi.service import Worker
from vumi.message import Message
from datetime import datetime, timedelta

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
        from vumi.campaigns import txtalert_mocking
        from twisted.python import log
        # we're expecting the client to initiate
        self.session_id = yield
        self.log('Starting new session for %s' % self.session_id)
        patient_id = yield self.ask('Welcome to txtAlert. ' \
                                'Please respond with your patient id.')
        # patient = Patient.objects.get(te_id=patient_id)
        self.log('patient_id: %s' % patient_id)
        patient = txtalert_mocking.Patient()
        self.log('patient: %s' % patient)
        visit = patient.next_visit()
        answer = yield self.choice( \
            'Welcome to txtAlert. ' \
            'Your next visit is on %s at %s' % (visit.date, visit.clinic),
            choices = (
                ('1', 'Change next appointment'),
                ('2', 'See attendance barometer'),
                ('3', 'Request call from clinic')
            ))
        self.log('first choice: %s' % answer)
        if answer == '1':
            answer = yield self.choice( \
                'Welcome to txtAlert. ' \
                'Appointment %s at %s' % (visit.date, visit.clinic),
                choices = (
                    ('1', 'Request earlier date'),
                    ('2', 'Request earlier time on %s' % visit.date),
                    ('3', 'Request later time on %s' % visit.date),
                    ('4', 'Request later date'),
                )
            )
            if answer == '1':
                visit.reschedule_earlier_date()
            elif answer == '2':
                visit.reschedule_earlier_time()
            elif answer == '3':
                visit.reschedule_later_time()
            elif answer == '4':
                visit.reschedule_later_date()
            yield self.close("Thanks! Your change request has been registered. " \
                        "You'll receive an SMS with your appointment information")
        elif answer == '2':
            yield self.close("Welcome to txtAlert.\n" \
                        "You have attended %s of your appointments, "\
                        "%s out of %s.\n" \
                        "%s appointments have been rescheduled.\n" \
                        "%s appointments missed.\n" % (patient.attendance, 
                            patient.attended,
                            patient.total,
                            patient.rescheduled,
                            patient.missed))
        elif answer == '3':
            yield self.close("Welcome to txtAlert. " \
                        "You have requested a call from the clinic. " \
                        "You will be called as soon as possible.")
        else:
            self.log('DEAD END!')
        

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
        start_of_conversation = coroutine.send(message) # announcement
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
        self.publisher.publish_message(Message(recipient=sender, message=response))
    
    @inlineCallbacks
    def stopWorker(self):
        yield None
