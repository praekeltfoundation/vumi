from twisted.internet.defer import inlineCallbacks, Deferred
from generator_tools import picklegenerators
from twisted.internet import stdio
from twisted.python import log
from twisted.protocols import basic
from vumi.service import Worker
from vumi.message import Message
from datetime import datetime

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
    
    def run(self):
        from vumi.campaigns import txtalert_mocking
        from twisted.python import log
        # we're expecting the client to initiate
        print 'yielding for announce'
        announce = yield
        print 'announce', announce
        patient_id = yield self.ask('Welcome to txtAlert. ' \
                                'Please respond with your patient id.')
        # patient = Patient.objects.get(te_id=patient_id)
        print 'patient_id', patient_id
        patient = txtalert_mocking.Patient()
        print 'patient ->', patient
        visit = patient.next_visit()
        answer = yield self.choice( \
            'Welcome to txtAlert. ' \
            'Your next visit is on %s at %s' % (visit.date, visit.clinic),
            choices = (
                ('1', 'Change next appointment'),
                ('2', 'See attendance barometer'),
                ('3', 'Request call from clinic')
            ))
        print 'first choice: ', answer
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
            log.msg('DEAD END!')
        

class BookingTool(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' 
                                                % self.config['username'])
        self.consume('xmpp.inbound.gtalk.%s' % self.config['username'], 
                        self.consume_message)
        self.sessions = {}
        
    def session_exists(self, uuid):
        return uuid in self.sessions
        
    def save_session(self, uuid, menu, coroutine):
        self.sessions[uuid] = picklegenerators.dumps((menu, coroutine))
    
    def get_session(self, uuid):
        return picklegenerators.loads(self.sessions[uuid])
    
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
        print 'menu finished?', menu.finished
        if menu.finished:
            return self.start_new_session(uuid, message)
        else:
            print 'answer:', message
            try:
                print 'continuing'
                response = coroutine.send(message)
                self.save_session(uuid, menu, coroutine)
            except TypeError, e:
                print 'restarting'
                self.save_session(uuid, menu, coroutine)
                restart = coroutine.next()
                response = coroutine.send(message)
            print 'response:', response
            return response
        
    def consume_message(self, message):
        sender = message.payload['sender']
        message = message.payload['message']
        # it's a new session
        if not self.session_exists(sender):
            response = self.start_new_session(sender, message)
        else:
            response = self.resume_existing_session(sender, message)
        self.publisher.publish_message(Message(recipient=sender, message=response))
    
    @inlineCallbacks
    def stopWorker(self):
        yield None
