from twisted.python import log
from twisted.internet.defer import inlineCallbacks, returnValue
from vumi.workers.integrat.worker import IntegratWorker
from vumi.utils import safe_routing_key, http_request
from xml.etree import ElementTree
from urllib import urlencode

class OpenSearch(object):
    
    URL = 'http://en.wikipedia.org/w/api.php'
    NS = "{http://opensearch.org/searchsuggest2}"
    
    @inlineCallbacks
    def search(self, query, limit=10, namespace=0):
        query_params = {
            'search': str(query),
            'action': 'opensearch',
            'limit': str(limit),
            'namespace': str(namespace),
            'format': 'xml'
        }
        url = '%s?%s' % (self.URL, urlencode(query_params))
        response = yield http_request(url, '', {
            'User-Agent': 'Vumi HTTP Request'
        }, method='GET')
        returnValue(self.parse_xml(response))
    
    def parse_xml(self, xml):
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
    return '\n'.join([
        '%s. %s' % (idx, result['text']) for idx, result in enumerate(results,start)
    ])

    
class WikipediaWorker(IntegratWorker):
    
    SESSIONS = {}
    
    def new_session(self, data):
        session_id = data["transport_session_id"]
        self.SESSIONS[session_id] = {}
        self.reply(session_id, "What would you like to search Wikipedia for?")
    
    def resume_session(self, data):
        session_id = data['transport_session_id']
        msisdn = data['sender']
        response = data["message"].strip()
        if response.isdigit():
            self.handle_selection(session_id, msisdn, int(response))
        else:
            self.handle_search(session_id, msisdn, response)
    
    @inlineCallbacks
    def handle_search(self, session_id, msisdn, query):
        results = yield OpenSearch().search(query)
        if results:
            log.msg('results', results)
            self.SESSIONS[session_id]['results'] = results
            self.reply(session_id, pretty_print_results(results))
        else:
            self.end(session_id, 'Sorry, no Wikipedia results for %s' % query)
    
    def handle_selection(self, session_id, msisdn, number):
        try:
            log.msg('user typed number', number)
            log.msg('sesson', self.SESSIONS[session_id])
            interest = self.SESSIONS[session_id]['results'][number - 1]
            self.end(session_id, 
                        '%s: %s...\nFull text will be delivered to you via SMS' % 
                        (
                            interest['text'],
                            interest['description'][:100]
                        ))
        except (KeyError, IndexError):
            self.end(session_id, 'Sorry, invalid selection. Please dial in again')
    
    def close_session(self, data):
        self.SESSIONS.pop(data['transport_session_id'], None)
    
# http://en.wikipedia.org/w/api.php?action=opensearch&search=Cell%20phone&limit=10&namespace=0&format=xmlfm