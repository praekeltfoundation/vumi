from vumi.services.truteq.base import Publisher, Consumer, SessionType
from vumi.services.worker import PubSubWorker
from twisted.python import log

from alexandria.client import Client
from alexandria.sessions.manager import SessionManager
from alexandria.dsl.core import MenuSystem, prompt, end
from alexandria.dsl.validators import pick_one

class Backend(object):
    """
    An in memory backend, this should sometime be made a persistent backend
    for the campaigns. Neither Vumi or Alexandria care what type of 
    backend is used.
    """
    def __init__(self):
        self.memory = {}
    
    def get_client_id(self, client):
        return "%(uuid)s-%(client_type)s" % client.uuid()
    
    def restore(self, client):
        uuid = self.get_client_id(client)
        state = self.memory.setdefault(uuid, {})
        log.msg("restoring state of %s as %s" % (uuid, state))
        return state
    
    def deactivate(self, client):
        uuid = self.get_client_id(client)
        log.msg("deactivating %s" % uuid)
        del self.memory[uuid]
    
    def save(self, client, state):
        uuid = self.get_client_id(client)
        log.msg("saving state of %s as %s" % (uuid, state))
        self.memory[uuid] = state
        return state
    

class StatefulClient(Client):
    """
    A client for Alexandria that stores its data in the in memory backend. The
    msisdn is used as a unique id to retrieve the client again from the 
    storage. The reply callback links back to the Consumer's reply instance
    which publishes to the queue.
    
    FIXME:  a publishing consumer? Confusing, this should be one PubSub class 
            and all this logic should be in the Worker class, not in the 
            consumer
    """
    def __init__(self, msisdn, reply_callback):
        self.id = msisdn
        self.reply_callback = reply_callback
        self.session_manager = SessionManager(client=self, backend=Backend())
        self.session_manager.restore()
    
    def send(self, message, end_of_session):
        if end_of_session:
            reply_type = SessionType.end
        else:
            reply_type = SessionType.existing
        self.reply_callback(self.id, message, reply_type)
    

class VumiConsumer(Consumer):
    
    """
    Describe the menu system we're running
    """
    menu = MenuSystem(
        prompt('What is your favorite programming language?', options=(
        'java', 'c', 'python', 'ruby', 'javascript', 'php', 'other'
        ), validator=pick_one),
        prompt('What is your favorite development operating system?', options=(
            'windows', 'apple', '*nix', 'other'
        ), validator=pick_one),
        prompt('What is your favorite development environment?', options=(
            'netbeans', 'eclipse', 'vim', 'emacs', 'textmate', 'notepad'
        ), validator=pick_one),
        end('Thanks! You have completed the quiz')
    )
    
    """
    In memory storage of stateful clients
    """
    clients = {}
    
    def new_client(self, msisdn):
        client = StatefulClient(msisdn, self.reply)
        self.clients[msisdn] = client
        return client
    
    def get_client(self, msisdn):
        if msisdn in self.clients:
            return self.clients[msisdn]
        else:
            return self.new_client(msisdn)
    
    def remove_client(self, msisdn):
        if msisdn in self.clients:
            del self.clients[msisdn]
    
    def new_ussd_session(self, msisdn, message):
        client = self.new_client(msisdn)
        client.answer(str(message), self.menu)
    
    def existing_ussd_session(self, msisdn, message):
        client = self.get_client(msisdn)
        client.answer(str(message), self.menu)
    
    def timed_out_ussd_session(self, msisdn, message):
        log.msg("%s timed out" % msisdn)
        self.remove_client(msisdn)
    
    def end_ussd_session(self, msisdn, message):
        log.msg("%s ended session" % msisdn)
        self.remove_client(msisdn)
    

class VumiUSSDWorker(PubSubWorker):
    consumer_class = VumiConsumer
    publisher_class = Publisher
