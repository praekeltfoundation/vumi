from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.web.server import Site
from vumi.utils import http_request
from vumi.message import TransportUserMessage
from vumi.transports.integrat.integrat import (IntegratHttpResource,
                                               IntegratTransport)


XML_TEMPLATE = '''
 <Message>
    <Version Version="1.0"/>
    <Response Type="OnUSSEvent">
        <SystemID>Higate</SystemID>
        <UserID>LoginName</UserID>
        <Service>SERVICECODE</Service>
        <Network ID="1" MCC="655" MNC="001"/>
        <OnUSSEvent Type="%(uss_type)s">
            <USSContext SessionID="%(sid)s" NetworkSID="%(network_sid)s" MSISDN="%(msisdn)s" Script="testscript" ConnStr="%(connstr)s"/>
            <USSText Type="TEXT">%(text)s</USSText>
        </OnUSSEvent>
    </Response>
</Message>
'''


class TestIntegratHttpResource(TestCase):

    DEFAULT_MSG = {
        'from_addr': '+2799053421',
        'to_addr': '*120*44#',
        'transport_metadata': {
            'session_id': 'sess1234',
            },
        }

    @inlineCallbacks
    def setUp(self):
        self.msgs = []
        site_factory = Site(IntegratHttpResource(self._publish))
        self.server = yield reactor.listenTCP(0, site_factory)
        addr = self.server.getHost()
        self._server_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.server.loseConnection()

    def _publish(self, **kws):
        self.msgs.append(kws)

    def send_request(self, xml):
        d = http_request(self._server_url, xml, method='GET')
        return d

    @inlineCallbacks
    def check_response(self, xml, responses):
        """Check that sending the given XML results in the given responses."""
        yield self.send_request(xml)
        for msg, expected_override in zip(self.msgs, responses):
            expected = self.DEFAULT_MSG.copy()
            expected.update(expected_override)
            for key, value in expected.items():
                self.assertEqual(msg[key], value)
        self.assertEqual(len(self.msgs), len(responses))
        del self.msgs[:]

    def make_ussd(self, ussd_type, sid=None, network_sid="netsid12345",
                  msisdn=None, connstr=None, text=""):
        sid = (self.DEFAULT_MSG['transport_metadata']['session_id']
              if sid is None else sid)
        msisdn = self.DEFAULT_MSG['from_addr'] if msisdn is None else msisdn
        connstr = self.DEFAULT_MSG['to_addr'] if connstr is None else connstr
        return XML_TEMPLATE % {
            'uss_type': ussd_type, 'sid': sid, 'network_sid': network_sid,
            'msisdn': msisdn, 'connstr': connstr, 'text': text,
            }

    @inlineCallbacks
    def test_new_session(self):
        xml = self.make_ussd(ussd_type='New', text="")
        yield self.check_response(xml, [])

    @inlineCallbacks
    def test_open_session(self):
        xml = self.make_ussd(ussd_type='Open', text="")
        yield self.check_response(xml, [{
            'session_event': TransportUserMessage.SESSION_NEW,
            'content': None,
            }])

        xml = self.make_ussd(ussd_type='Request', text="REQ")
        yield self.check_response(xml, [{
            'session_event': TransportUserMessage.SESSION_NEW,
            'content': None,
            }])

    @inlineCallbacks
    def test_resume_session(self):
        xml = self.make_ussd(ussd_type='Request', text="foo")
        yield self.check_response(xml, [{
            'session_event': TransportUserMessage.SESSION_RESUME,
            'content': 'foo',
            }])

    @inlineCallbacks
    def test_non_ussd(self):
        xml = """
        <Message>
            <Version Version="1.0"/>
            <Response Type="OnReceiveSMS">
            <OnReceiveSMS>
              <Content Type="HEX">06052677F6A565 ...etc</Content>
            </OnReceiveSMS>
            </Response>
        </Message>
        """
        yield self.check_response(xml, [])


class TestIntegratTransport(TestCase):

    def test_health(self):
        pass
