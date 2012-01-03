# -*- encoding: utf-8 -*-

from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site

from vumi.utils import http_request
from vumi.tests.utils import get_stubbed_worker, MockHttpServer
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
        <OnUSSEvent Type="%(ussd_type)s">
            <USSContext SessionID="%(sid)s"
                        NetworkSID="%(network_sid)s"
                        MSISDN="%(msisdn)s"
                        Script="testscript"
                        ConnStr="%(connstr)s"/>
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
        site_factory = Site(IntegratHttpResource("testgrat", "ussd",
            self._publish))
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
            'ussd_type': ussd_type, 'sid': sid, 'network_sid': network_sid,
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

    timeout = 5

    @inlineCallbacks
    def setUp(self):
        self.integrat_calls = DeferredQueue()
        self.mock_integrat = MockHttpServer(self.handle_request)
        yield self.mock_integrat.start()
        config = {
            'transport_name': 'testgrat',
            'web_path': "foo",
            'web_port': "0",
            'url': self.mock_integrat.url,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.worker = get_stubbed_worker(IntegratTransport, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()
        addr = self.worker.web_resource.getHost()
        self.worker_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()
        yield self.mock_integrat.stop()

    def handle_request(self, request):
        self.integrat_calls.put(request)
        return ''

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.worker_url + "health", "",
                                    method='GET')
        self.assertEqual(result, "OK")

    @inlineCallbacks
    def test_outbound(self):
        msg = TransportUserMessage(to_addr="12345", from_addr="56789",
                                   transport_name="testgrat",
                                   transport_type="ussd",
                                   transport_metadata={
                                       'session_id': "sess123",
                                       },
                                   )
        self.broker.publish_message("vumi", "testgrat.outbound", msg)
        req = yield self.integrat_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.getHeader('content-type'),
                         'text/xml; charset=utf-8')
        self.assertEqual(req.content.getvalue(),
                         '<Message><Version Version="1.0" />'
                         '<Request Flags="0" SessionID="sess123"'
                           ' Type="USSReply">'
                         '<UserID Orientation="TR">testuser</UserID>'
                         '<Password>testpass</Password>'
                         '<USSText Type="TEXT" />'
                         '</Request></Message>')

    @inlineCallbacks
    def test_inbound(self):
        xml = XML_TEMPLATE % {
            'ussd_type': 'Request',
            'sid': 'sess1234',
            'network_sid': "netsid12345",
            'msisdn': '27345',
            'connstr': '*120*99#',
            'text': 'foobar',
            }
        yield http_request(self.worker_url + "foo", xml, method='GET')
        msg, = yield self.broker.wait_messages("vumi", "testgrat.inbound", 1)
        payload = msg.payload
        self.assertEqual(payload['transport_name'], "testgrat")
        self.assertEqual(payload['transport_type'], "ussd")
        self.assertEqual(payload['transport_metadata'],
                         {"session_id": "sess1234"})
        self.assertEqual(payload['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(payload['from_addr'], '27345')
        self.assertEqual(payload['to_addr'], '*120*99#')
        self.assertEqual(payload['content'], 'foobar')

    @inlineCallbacks
    def test_inbound_non_ascii(self):
        xml = (XML_TEMPLATE % {
            'ussd_type': 'Request',
            'sid': 'sess1234',
            'network_sid': "netsid12345",
            'msisdn': '27345',
            'connstr': '*120*99#',
            'text': u'öæł',
            }).encode("utf-8")
        yield http_request(self.worker_url + "foo", xml, method='GET')
        msg, = yield self.broker.wait_messages("vumi", "testgrat.inbound", 1)
        payload = msg.payload
        self.assertEqual(payload['content'], u'öæł')
