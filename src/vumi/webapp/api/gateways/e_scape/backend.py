from vumi.webapp.api.utils import callback
import xml.etree.ElementTree as ET
from collections import namedtuple

class E_ScapeException(Exception): pass

class E_ScapeReponse(object):
    recipient = None
    sender = None
    status = None
    smsc = None
    text = None

class E_Scape(object):
    
    def __init__(self, api_id):
        """
        *   api_id is assigned to you by E-scape
        *   if smsc is provided then this is used as the default for all
            messages sent unless it's specified again when calling send_sms
        """
        self.api_id = api_id
        self.gateway_url = 'http://site.demoru.com/api.php'
    
    def send_sms(self, smsc, sender, recipients, text):
        """
        We mimick a FORM post to the given URL and get back an HTML table.
        Got this from http://site.demoru.com/?page_id=27
        
        HTTP 200 OK
        Date	Thu: 03 Jun 2010 18:32:15 GMT
        Connection	Close
        Transfer-Encoding	chunked
        Content-Type	text/html; charset=UTF-8
        
        <table class="widefat" border="0">
            <thead>
                <tr>
                    <th>from</th>
                    <th>to</th>
                    <th>smsc</th>
                    <th>status</th>
                    <th>text</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td>+35566</td>
                    <td>+44778962937</td>
                    <td>ESC-P1Celtel</td>
                    <td>0: Accepted for delivery</td>
                    <td> http://www.mobi-fee.com/link/g.lnk?ID=135</td>
                </tr>
            </tbody>
            <tfoot>
                <tr>
                    <th>from</th>
                    <th>to</th>
                    <th>smsc</th>
                    <th>status</th>
                    <th>text</th>
                </tr>
            </tfoot>
        </table>
                
        """
        if not all([msisdn.startswith('+') for msisdn in recipients]):
            raise E_ScapeException, 'All msisdns should start with a +'
        kwargs = {
            's': sender,
            'r': ','.join(recipients),
            'text': text,
            'smsc': smsc,
            'api_id': self.api_id,
            'send': 'go' # apparently the form submission key
        }
        return parse_response(callback(self.gateway_url, kwargs.items()))
    
    def parse_response(self, response):
        tree = ET.fromstring(response)
        rows = tree.findall('tbody/tr')
        if not rows:
            raise E_ScapeException('Unparsable response: %s' % response)
        
        responses = []
        for row in rows:
            sender, recipient, smsc, status, text = [td.text for td in row.findall('td')]
            response = E_ScapeReponse()
            response.sender = sender
            response.recipient = recipient
            response.smsc = smsc
            response.status = [s.strip() for s in status.split(':')]
            response.text = text
            responses.append(response)
        return responses
        
