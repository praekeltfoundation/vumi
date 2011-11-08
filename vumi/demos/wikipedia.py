# -*- test-case-name: vumi.demos.tests.test_wikipedia -*-

from twisted.internet.defer import inlineCallbacks, returnValue
from vumi.application import ApplicationWorker, SessionManager
from vumi.utils import http_request, get_deploy_int
from xml.etree import ElementTree
from urllib import urlencode
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

    MAX_SESSION_LENGTH = 3 * 60

    @inlineCallbacks
    def startWorker(self):
        self.session_manager = SessionManager(
            get_deploy_int(self._amqp_client.vhost),
            "%(worker_name)s:%(transport_name)s" % self.config,
            max_session_length=self.MAX_SESSION_LENGTH)

        yield super(WikipediaWorker, self).startWorker()

    @inlineCallbacks
    def stopWorker(self):
        yield self.session_manager.stop()
        yield super(WikipediaWorker, self).stopWorker()

    def consume_user_message(self, msg):
        user_id = msg.user()
        session = self.session_manager.load_session(user_id)
        if session and msg['content'] is not None:
            self.resume_wikipedia_session(msg, session)
        else:
            session = self.session_manager.create_session(user_id)
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
            self.session_manager.save_session(msg.user(), session)
        else:
            self.reply_to(msg, 'Sorry, no Wikipedia results for %s' % query,
                False)
            self.session_manager.clear_session(msg.user())

    def handle_selection(self, msg, session, number):
        try:
            results = json.loads(session['results'])
            interest = results[number - 1]
            self.reply_to(msg,
                     '%s: %s...\nFull text will be delivered to you via SMS' %
                     (interest['text'], interest['description'][:100]), False)
        except (KeyError, IndexError):
            self.reply_to(msg,
                'Sorry, invalid selection. Please restart and try again',
                False)
        self.session_manager.clear_session(msg.user())
