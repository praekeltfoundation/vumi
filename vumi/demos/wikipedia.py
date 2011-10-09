# -*- test-case-name: vumi.demos.tests.test_wikipedia -*-

from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import task
from vumi.application import ApplicationWorker
from vumi.utils import http_request, get_deploy_int
from xml.etree import ElementTree
from urllib import urlencode
from datetime import timedelta, datetime
import time
import redis
import time
import json


class OpenSearch(object):
    """
    A tiny helper class that gets search suggestions back from Wikipedia's
    OpenSearch SearchSuggest2 implementation
    """

    URL = 'http://en.wikipedia.org/w/api.php'
    NS = "{http://opensearch.org/searchsuggest2}"

    @inlineCallbacks
    def search(self, query, limit=10, namespace=0):
        """
        Perform a query and returns a list of dictionaries with results 
        matching the query.
        
        Parameters
        ----------
        query : str
            The search term.
        limit : int, optional
            How many results to get back, defaults to 10
        namespace : int, optional
            The namespace of the OpenSearch Suggestions extention, defaults
            to 0
        
        """
        query_params = {
            'search': query.encode('utf-8'),
            'action': 'opensearch',
            'limit': str(limit),
            'namespace': str(namespace),
            'format': 'xml',
        }
        url = '%s?%s' % (self.URL, urlencode(query_params))
        response = yield http_request(url, '', {
            'User-Agent': 'Vumi HTTP Request',
        }, method='GET')
        returnValue(self.parse_xml(response))

    def parse_xml(self, xml):
        """
        Parse the OpenSearch SearchSuggest XML result response and return a 
        list of dictionaries containing the results.
        """
        root = ElementTree.fromstring(xml)
        section = root.find('%sSection' % self.NS)
        items = section.findall('%sItem' % self.NS)
        return [{
            'text': text(item, '%sText' % self.NS),
            'description': text(item, '%sDescription' % self.NS),
            'url': text(item, '%sUrl' % self.NS),
            'image': {
                'source': image(item, '%sImage' % self.NS).get('source'),
                'width': image(item, '%sImage' % self.NS).get('width'),
                'height': image(item, '%sImage' % self.NS).get('height'),
            }
        } for item in items]


def text(item, element):
    try:
        return item.find(element).text
    except AttributeError:
        return ''


def image(item, element):
    el = item.find(element)
    return getattr(el, 'attrib', {})


def pretty_print_results(results, start=1):
    """
    Turn a list of results into an enumerate multiple choice list
    """
    return '\n'.join(['%s. %s' % (idx, result['text'])
                      for idx, result in enumerate(results, start)])

class SessionApplicationWorker(ApplicationWorker):
    
    # How long a session is allowed to last in seconds. Defaults to `None` 
    # which means sessions never expire
    MAX_SESSION_LENGTH = None
    
    @inlineCallbacks
    def startWorker(self):
        # Connect to Redis
        redis_conf = self.config.get('redis', {})
        self.r_server = redis.Redis(
            db=get_deploy_int(self._amqp_client.vhost), **redis_conf)
        self.r_prefix = "%(worker_name)s:%(transport_name)s" % self.config
        
        yield super(SessionApplicationWorker, self).startWorker()
    
    def active_sessions(self):
        """
        Return a list of active user_ids and associated sessions. Loops over
        known active_sessions, some of which might have auto expired. 
        Implements lazy garbage collection, for each entry it checks if 
        the user's session still exists, if not it is removed from the set.
        """
        skey = self.r_key('active_sessions')
        for user_id in self.r_server.smembers(skey):
            ukey = self.r_key('session', user_id)
            if self.r_server.exists(ukey):
                yield user_id, self.load_session(user_id)
            else:
                self.r_server.srem(skey, user_id)
    
    def r_key(self, *args):
        """
        Generate a keyname using this workers prefix
        """
        parts = [self.r_prefix]
        parts.extend(args)
        return ":".join(parts)

    def load_session(self, user_id):
        """
        Load session data from Redis
        """
        ukey = self.r_key('session', user_id)
        return self.r_server.hgetall(ukey)

    def schedule_session_expiry(self, user_id, timeout):
        """
        Schedule a session to timeout
        
        Parameters
        ----------
        user_id : str
            The user's id.
        timeout : int
            The number of seconds after which this session should expire
        
        """
        ukey = self.r_key('session', user_id)
        log.msg('%s expires in %s' % (ukey, timeout))
        self.r_server.expire(ukey, timeout)
    
    def create_session(self, user_id):
        """
        Create a new session using the given user_id
        """
        session = self.save_session(user_id, {
            'created_at': time.time()
        })
        if self.MAX_SESSION_LENGTH:
            self.schedule_session_expiry(user_id, self.MAX_SESSION_LENGTH)
        return session

    def clear_session(self, user_id):
        log.msg('clearing session for %s' % user_id)
        ukey = self.r_key('session', user_id)
        self.r_server.delete(ukey)

    def save_session(self, user_id, session):
        """
        Save a session
        
        Parameters
        ----------
        user_id : str
            The user's id.
        session : dict
            The session info, nested dictionaries are not supported. Any 
            values that are dictionaries are converted to strings by Redis.
        
        """
        ukey = self.r_key('session', user_id)
        for s_key, s_value in session.items():
            self.r_server.hset(ukey, s_key, s_value)
        skey = self.r_key('active_sessions')
        self.r_server.sadd(skey, user_id)
        return session


class WikipediaWorker(SessionApplicationWorker):
    
    MAX_SESSION_LENGTH = 3 * 60
    
    def consume_user_message(self, msg):
        user_id = msg.user()
        session = self.load_session(user_id)
        if session:
            self.resume_wikipedia_session(msg, session)
        else:
            session = self.create_session(user_id)
            self.new_wikipedia_session(msg, session)
    
    def new_wikipedia_session(self, msg, session):
        self.reply_to(msg, "What would you like to search Wikipedia for?",
            True)

    def resume_wikipedia_session(self, msg, session):
        response = msg['content'].strip()
        if response.isdigit():
            self.handle_selection(msg, session, int(response))
        else:
            self.handle_search(msg, session, response)

    @inlineCallbacks
    def handle_search(self, msg, session, query):
        results = yield OpenSearch().search(query)
        if results:
            session['results'] = json.dumps(results)
            self.reply_to(msg, pretty_print_results(results), True)
            self.save_session(msg.user(), session)
        else:
            self.reply_to(msg, 'Sorry, no Wikipedia results for %s' % query, 
                False)
            self.clear_session(msg.user())

    def handle_selection(self, msg, session, number):
        try:
            results = json.loads(session['results'])
            interest = results[number - 1]
            self.reply_to(msg,
                     '%s: %s...\nFull text will be delivered to you via SMS' %
                     (interest['text'], interest['description'][:100]), False)
        except (KeyError, IndexError):
            self.reply_to(msg,
                     'Sorry, invalid selection. Please dial in again', False)
        self.clear_session(msg.user())
