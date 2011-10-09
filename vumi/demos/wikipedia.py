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


class WikipediaWorker(ApplicationWorker):
    
    MAX_SESSION_LENGTH = timedelta(minutes=3)
    
    @inlineCallbacks
    def startWorker(self):
        # Connect to Redis
        redis_conf = self.config.get('redis', {})
        self.r_server = redis.Redis(redis_conf.get('host', 'localhost'),
                                db=get_deploy_int(self._amqp_client.vhost))
        self.r_server.flushdb()
        log.msg("Connected to Redis")
        self.r_prefix = "wikipedia:%s" % (self.config['transport_name'],)
        
        self.gc = task.LoopingCall(self.garbage_collect_expired_sessions)
        self.gc.start(1)
        
        yield super(WikipediaWorker, self).startWorker()
    
    def garbage_collect_expired_sessions(self):
        skey = self.r_key('active_sessions')
        for user_id in self.r_server.smembers(skey):
            session = self.load_session(user_id)
            timestamp = datetime.fromtimestamp(float(session['created_at']))
            expiry = datetime.now() - self.MAX_SESSION_LENGTH
            if timestamp < expiry:
                self.clear_session(user_id)
            else:
                log.msg('%s left for %s' % (timestamp - expiry, user_id))
    
    def r_key(self, *args):
        parts = [self.r_prefix]
        parts.extend(args)
        return ":".join(parts)
    
    def load_session(self, user_id):
        ukey = self.r_key('session', user_id)
        return self.r_server.hgetall(ukey)
    
    def create_session(self, user_id):
        return self.save_session(user_id, {
            'created_at': time.time()
        })
    
    def clear_session(self, user_id):
        log.msg('clearing session for %s' % user_id)
        ukey = self.r_key('session', user_id)
        self.r_server.delete(ukey)
        skey = self.r_key('active_sessions')
        self.r_server.srem(skey, user_id)
    
    def save_session(self, user_id, session):
        ukey = self.r_key('session', user_id)
        for s_key, s_value in session.items():
            self.r_server.hset(ukey, s_key, s_value)
        skey = self.r_key('active_sessions')
        self.r_server.sadd(skey, user_id)
        return session
    
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


# http://en.wikipedia.org/w/api.php?action=opensearch&search=Cell%20phone&
# limit=10&namespace=0&format=xmlfm
