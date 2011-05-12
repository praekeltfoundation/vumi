from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import Message
from vumi.service import Worker, Consumer, Publisher
from vumi.workers.truteq.util import VumiSSMIFactory, SessionType, ussd_code_to_routing_key

from alexandria.client import Client
from alexandria.sessions.backend import DBBackend
from alexandria.sessions.manager import SessionManager
from alexandria.sessions.db import models
from alexandria.sessions.db.views import _get_data
from alexandria.dsl.core import MenuSystem, prompt, end, case#, sms
from alexandria.dsl.validators import pick_one


PHONE_OPTIONS = (
    'Nokia',
    'iPhone',
    'Blackberry',
    'Samsung',
    'Android',
    'Other',
)

TECHNOLOGY_OPTIONS = (
    'SMS',
    'USSD',
    'Please Call Me',
    'Mobi',
    'Web',
)

USEFUL_OPTIONS = (
    'Banking',
    'Health',
    'Education',
    'Security',
    'Marketing',
    'Entertainment'
)

CONTINUE_OR_QUIT_OPTIONS = (
    'Continue',
    'End the session'
)

QUIT_MESSAGE = \
    'Thanks for taking part. You can view real-time statistics on ' + \
    'the Praekelt screens, or by dialing back into the Star menu ' + \
    'system!'

def sms(text):
    """
    Send an SMS to the current session's MSISDN.
    Not working: see comment below
    """
    while True:
        ms, session = yield
        # in it's current form this isn't going to work since
        # these items don't have access to the queue and cannot
        # publish anything.

class VumiDBClient(Client):
    
    def __init__(self, msisdn, send_ussd_callback, send_sms_callback):
        self.id = msisdn
        self.session_manager = SessionManager(client=self, backend=DBBackend())
        self.session_manager.restore()
        self.send_ussd_callback = send_ussd_callback
        self.send_sms_callback = send_sms_callback
    
    def send(self, text, end_session=False):
        if end_session:
            reply_type = SessionType.END
        else:
            reply_type = SessionType.EXISTING
        return self.send_ussd_callback(self.id, text, reply_type)
    
    def send_sms(self, text):
        return self.send_sms_callback(self.id, text)

def persist(key, value, *args, **kwargs):
    """
    Save a key, value in the session. If value is a callable
    then the result of value(*args, **kwargs) will be saved in the session
    """
    while True:
        ms, session = yield
        if callable(value):
            session[key] = value(*args, **kwargs)
        else:
            session[key] = value
        yield False, False

def calculate_stats(key, options):
    """
    Get the statistics for the given key from the session. Abuses the view
    from alexandria to do the database calculation. Sorry, very ugly.
    """
    # get data
    data = _get_data().get(key, {})
    # calculate the total nr of entries
    total = float(sum(data.values()))
    if total:
        # return a list of key: % values
        return "\n".join([
            "%s: %.0f%%" % (
                option, 
                (data.get(option, 0) / total) * 100
            ) for option in options
        ])
    else:
        return "Not enough data yet"

def returning_user(menu, session):
    return session.get('completed', False)

def new_user(*args, **kwargs):
    return not returning_user(*args, **kwargs)

def wants_to_quit(menu, session):
    # 2. End the session, check if that was answered
    return session.pop('continue_or_quit', None) == '2'

class VumiConsumer(Consumer):
    """
    This consumer consumes all incoming USSD messages on the *120*663*79#
    shortcode in a campaign specific queue.
    """
    exchange_name = "vumi.ussd"
    exchange_type = "topic"
    durable = False
    queue_name = "ussd.truteq.test_campaign"
    routing_key = "ussd.s120s663s79h"
    
    
    """
    Describe the menu system we're running
    """
    menu = MenuSystem(
        case(
            (new_user, prompt('Welcome to the Praekelt Star menu system. ' +\
                                    'What is your first name?', save_as='name')),
            (returning_user, prompt('Welcome back %(name)s! Continue to ' +\
                                    'see the real-time statistics.', 
                                        parse=True,
                                        options=CONTINUE_OR_QUIT_OPTIONS))
        ),
        case(
            (wants_to_quit, end(QUIT_MESSAGE)),
        ),
        persist('phone_stats', calculate_stats, 'phone', PHONE_OPTIONS),
        case(
            (new_user, prompt('What type of phone do you have?', 
                                options=PHONE_OPTIONS, 
                                save_as='phone', 
                                validator=pick_one)),
            (returning_user, prompt("%(phone_stats)s", 
                                        parse=True,
                                        save_as='continue_or_quit',
                                        options=CONTINUE_OR_QUIT_OPTIONS))
        ),
        case(
            (wants_to_quit, end(QUIT_MESSAGE)),
        ),
        persist('technology_stats', calculate_stats, 'technology', TECHNOLOGY_OPTIONS),
        case(
            (new_user, prompt('Which technology is most suited to your audience?', 
                                options=TECHNOLOGY_OPTIONS, 
                                save_as='technology', 
                                validator=pick_one)),
            (returning_user, prompt("%(technology_stats)s", 
                                        parse=True, 
                                        save_as='continue_or_quit',
                                        options=CONTINUE_OR_QUIT_OPTIONS))
        ),
        case(
            (wants_to_quit, end(QUIT_MESSAGE)),
        ),
        persist('useful_stats', calculate_stats, 'useful', USEFUL_OPTIONS),
        case(
            (new_user, prompt('What is the most useful application of mobile technology for lower income markets?', 
                                options=USEFUL_OPTIONS, 
                                save_as='useful', 
                                validator=pick_one)),
            (returning_user, prompt("%(useful)s", 
                                        parse=True, 
                                        save_as='continue_or_quit',
                                        options=CONTINUE_OR_QUIT_OPTIONS))
        ),
        case(
            (wants_to_quit, end(QUIT_MESSAGE)),
        ),
        persist('completed', True),
        # sms(
        #     'Hi %(name)s want to know more about Vumi and Star menus? ' + \
        #     'Visit http://www.praekelt.com'
        # ),
        end(QUIT_MESSAGE)
    )
    
    def __init__(self, publisher):
        self.publisher = publisher
    
    def consume_message(self, message):
        log.msg("Consumed Message %s" % message)
        dictionary = message.payload
        ussd_type = dictionary.get('ussd_type')
        msisdn = dictionary.get('msisdn')
        message = dictionary.get('message')
        if(ussd_type == SessionType.NEW):
            self.new_ussd_session(msisdn, message)
        elif(ussd_type == SessionType.EXISTING):
            self.existing_ussd_session(msisdn, message)
        elif(ussd_type == SessionType.TIMEOUT):
            self.timed_out_ussd_session(msisdn, message)
        elif(ussd_type == SessionType.END):
            self.end_ussd_session(msisdn, message)
    
    def new_ussd_session(self, msisdn, message):
        client = VumiDBClient(msisdn, self.reply, self.reply_with_sms)
        client.answer(str(message), self.menu)
    
    def existing_ussd_session(self, msisdn, message):
        client = VumiDBClient(msisdn, self.reply, self.reply_with_sms)
        client.answer(str(message), self.menu)
    
    def timed_out_ussd_session(self, msisdn, message):
        log.msg('%s timed out, removing client' % msisdn)
        client = VumiDBClient(msisdn, self.reply, self.reply_with_sms)
        client.deactivate()
    
    def end_ussd_session(self, msisdn, message):
        log.msg('%s ended the session, removing client' % msisdn)
        client = VumiDBClient(msisdn, self.reply, self.reply_with_sms)
        client.deactivate()
    
    def reply_with_sms(self, msisdn, message):
        return self.publisher.send({
            "type": "sms",
            "msisdn": msisdn,
            "message": message
        })
    
    def reply(self, msisdn, message, ussd_type):
        return self.publisher.publish_message(Message(**{
            "ussd_type": ussd_type,
            "msisdn": msisdn,
            "message": message
        }))
    

class VumiPublisher(Publisher):
    exchange_name = "vumi.ussd"
    exchange_type = "topic"             # -> route based on pattern matching
    routing_key = 'ussd.truteq.s120s663s79h'
    durable = False                     # -> not created at boot
    auto_delete = False                 # -> auto delete if no consumers bound
    delivery_mode = 2                   # -> do not save to disk

    def publish_message(self, message, **kwargs):
        log.msg("Publishing Message %s with extra args: %s" % (message, kwargs))
        super(VumiPublisher, self).publish_message(message, **kwargs)


class USSDWorker(Worker):
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the USSDWorker")
        self.publisher = yield self.start_publisher(VumiPublisher)
        self.consumer = yield self.start_consumer(VumiConsumer, self.publisher)
    
    def stopWorker(self):
        log.msg("Stopping the USSDWorker")



