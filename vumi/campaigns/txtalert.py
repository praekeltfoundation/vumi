from twisted.internet.defer import inlineCallbacks, Deferred
from twisted.internet import stdio
from twisted.python import log
from twisted.protocols import basic
from vumi.service import Worker
from vumi.message import Message
from datetime import datetime

"""Mocking classes for getting txtAlert USSD to work"""
class Visit(object):
    def __init__(self):
        self.date = datetime.now()
        self.clinic = 'Clinic A'
    
    def reschedule_earlier_date(self):
        return True
    def reschedule_earlier_time(self):
        return True
    def reschedule_later_time(self):
        return True
    def reschedule_later_date(self):
        return True
    

class Patient(object):
    def __init__(self):
        self.attendance = '85%'
        self.attended = 60
        self.total = 85
        self.rescheduled = 5
        self.missed = 15
    
    def next_visit(self):
        return Visit()

class Menu(object):
    def __init__(self, announce):
        self.msisdn = announce
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
        patient_id = yield self.ask('Welcome to txtAlert. ' \
                                'Please respond with your patient id.')
        # patient = Patient.objects.get(te_id=patient_id)
        patient = Patient()
        visit = patient.next_visit()
        answer = yield self.choice( \
            'Welcome to txtAlert. ' \
            'Your next visit is on %s at %s' % (visit.date, visit.clinic),
            choices = (
                ('1', 'Change next appointment'),
                ('2', 'See attendance barometer'),
                ('3', 'Request call from clinic')
            ))
        
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
            yield self.close("Thanks! Your change request has been registered." \
                        "You'll receive an SMS with your appointment information")
        elif answer == '2':
            yield self.close("Welcome to txtAlert. " \
                        "You have attended %s of your appointments, "\
                        "%s out of %s." \
                        "%s appointments have been rescheduled" \
                        "%s appointments missed" % (patient.attendance, 
                            patient.attended,
                            patient.total,
                            patient.rescheduled,
                            patient.missed))
        elif answer == '3':
            yield self.close("Welcome to txtAlert. " \
                        "You have requested a call from the clinic. " \
                        "You will be called as soon as possible.")
        

class BookingTool(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        self.publisher = yield self.publish_to('xmpp.outbound.gtalk.%s' 
                                                % self.config['username'])
        self.consume('xmpp.inbound.gtalk.%s' % self.config['username'], 
                        self.consume_message)
        self.sessions = {}
        
    
    def start_new_session(self, uuid, message):
        # assume the first message is the announcement
        menu = Menu(message)
        coroutine = menu.run()
        resp = coroutine.next()
        # store in memory, this is going to suck
        self.sessions[uuid] = (menu, coroutine)
        return resp
    
    def resume_existing_session(self, uuid, message):
        # grab from memory
        menu, coroutine = self.sessions[uuid]
        if menu.finished:
            return self.start_new_session(uuid, message)
        else:
            return coroutine.send(message)
        
    
    def consume_message(self, message):
        sender = message.payload['sender']
        message = message.payload['message']
        # it's a new session
        if sender not in self.sessions:
            response = self.start_new_session(sender, message)
        else:
            response = self.resume_existing_session(sender, message)
        self.publisher.publish_message(Message(recipient=sender, message=response))
    
    @inlineCallbacks
    def stopWorker(self):
        pass