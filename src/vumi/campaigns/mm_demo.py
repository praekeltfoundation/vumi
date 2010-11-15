from vumi.services.truteq.base import Publisher, Consumer, SessionType
from vumi.services.worker import PubSubWorker
from twisted.python import log

from alexandria.client import Client
from alexandria.sessions.backend import DBBackend
from alexandria.sessions.manager import SessionManager
from alexandria.sessions.db import models
from alexandria.sessions.db.views import _get_data
from alexandria.dsl.core import MenuSystem, prompt, end, case#, sms
from alexandria.dsl.validators import pick_one

"""
Issues that have surfaced when developing this:

1.  Vumi does work with different transports but using different 
    transports in the same menu does not work. For example; sending out an SMS
    in a USSD session. The menu items do not have access to the queue and
    so cannot publish any message to a consumer.

2.  We need a lazy evalation decorator somewhere. Right now I have to pass
    callables to make sure that stuff isn't evaluated too soon.

3.  Using the session for string substition is useful, but it's implementation
    with the explicit `parse=True` is silly.

4.  The case((check, response), ...) works great on a lowlevel but is far
    too wordy for everyday use.

5.  I suspect there should be two types of session storage, one operational 
    for things like stack counters and one for the application for storing
    things like responses to the questions. They should be able to be reset
    apart from each other.

6.  For a lot of the yield-ing I should probably be looking at something
    like eventlet / greenlet / gevent

"""

INDUSTRY_OPTIONS = (
    'Marketing',
    'Industry',
    'Retail',
    'Financial/Banking',
    'IT/Technology',
    'Media',
    'Other'
)

EXPECTATIONS_OPTIONS = (
    'Meeting my expectations',
    'Exceeding my expectations',
    'Not meeting my expectations',
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
            reply_type = SessionType.end
        else:
            reply_type = SessionType.existing
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
            session[key] = value()
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
        persist('industry_stats', calculate_stats, 'industry', INDUSTRY_OPTIONS),
        case(
            (new_user, prompt('What industry are you from?', 
                                options=INDUSTRY_OPTIONS, 
                                save_as='industry', 
                                validator=pick_one)),
            (returning_user, prompt("%(industry_stats)s", 
                                        parse=True,
                                        save_as='continue_or_quit',
                                        options=CONTINUE_OR_QUIT_OPTIONS))
        ),
        case(
            (wants_to_quit, end(QUIT_MESSAGE)),
        ),
        persist('expectations_stats', calculate_stats, 'expectations', EXPECTATIONS_OPTIONS),
        case(
            (new_user, prompt('How are you finding the conference?', 
                                options=EXPECTATIONS_OPTIONS, 
                                save_as='expectations', 
                                validator=pick_one)),
            (returning_user, prompt("%(expectations_stats)s", 
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
        
    

class VumiUSSDWorker(PubSubWorker):
    consumer_class = VumiConsumer
    publisher_class = Publisher
