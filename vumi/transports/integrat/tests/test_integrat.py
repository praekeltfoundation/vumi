# -*- encoding: utf-8 -*-

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site

from vumi.utils import http_request
from vumi.tests.utils import MockHttpServer
from vumi.message import TransportUserMessage
from vumi.transports.integrat.integrat import (IntegratHttpResource,
                                               IntegratTransport)
from vumi.transports.tests.helpers import TransportHelper
from vumi.tests.helpers import VumiTestCase


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


class TestIntegratHttpResource(VumiTestCase):

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
        self.server = yield reactor.listenTCP(
            0, site_factory, interface='127.0.0.1')
        self.add_cleanup(self.server.loseConnection)
        addr = self.server.getHost()
        self._server_url = "http://%s:%s/" % (addr.host, addr.port)

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
        # this should not generate a message since we use
        # the 'open' event to start a new sesison.
        xml = self.make_ussd(ussd_type='New', text="")
        yield self.check_response(xml, [])

    @inlineCallbacks
    def test_new_session_via_request(self):
        # this should not generate a message since we use
        # the 'open' event to start a new sesison.
        xml = self.make_ussd(ussd_type='Request', text="REQ")
        yield self.check_response(xml, [])

    @inlineCallbacks
    def test_open_session(self):
        xml = self.make_ussd(ussd_type='Open', text="")
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


class TestIntegratTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.integrat_calls = DeferredQueue()
        self.mock_integrat = MockHttpServer(self.handle_request)
        self.add_cleanup(self.mock_integrat.stop)
        yield self.mock_integrat.start()
        config = {
            'web_path': "foo",
            'web_port': "0",
            'url': self.mock_integrat.url,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.tx_helper = self.add_helper(TransportHelper(IntegratTransport))
        self.transport = yield self.tx_helper.get_transport(config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)
        self.higate_response = '<Response status_code="0"/>'

    def handle_request(self, request):
        # The content attr will have been set to None by the time we read this.
        request.content_body = request.content.getvalue()
        self.integrat_calls.put(request)
        return self.higate_response

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(result, "OK")

    @inlineCallbacks
    def test_outbound(self):
        yield self.tx_helper.make_dispatch_outbound("hi", transport_metadata={
            'session_id': "sess123",
        })
        req = yield self.integrat_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.getHeader('content-type'),
                         'text/xml; charset=utf-8')
        self.assertEqual(req.content_body,
                         '<Message><Version Version="1.0" />'
                         '<Request Flags="0" SessionID="sess123"'
                           ' Type="USSReply">'
                         '<UserID Orientation="TR">testuser</UserID>'
                         '<Password>testpass</Password>'
                         '<USSText Type="TEXT">hi</USSText>'
                         '</Request></Message>')

    @inlineCallbacks
    def test_outbound_no_content(self):
        yield self.tx_helper.make_dispatch_outbound(None, transport_metadata={
            'session_id': "sess123",
        })
        req = yield self.integrat_calls.get()
        self.assertEqual(req.path, '/')
        self.assertEqual(req.method, 'POST')
        self.assertEqual(req.getHeader('content-type'),
                         'text/xml; charset=utf-8')
        self.assertEqual(req.content_body,
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
        yield http_request(self.transport_url + "foo", xml, method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['transport_metadata'],
                         {"session_id": "sess1234"})
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['from_addr'], '27345')
        self.assertEqual(msg['to_addr'], '*120*99#')
        self.assertEqual(msg['content'], 'foobar')

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
        yield http_request(self.transport_url + "foo", xml, method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['content'], u'öæł')

    @inlineCallbacks
    def test_nack(self):
        self.higate_response = """
            <Response status_code="-1">
                <Data name="method_error">
                    <field name="error_code" value="-1"/>
                    <field name="reason" value="Expecting POST, not GET"/>
                </Data>
            </Response>""".strip()

        msg = yield self.tx_helper.make_dispatch_outbound(
            "hi", transport_metadata={'session_id': "sess123"})
        yield self.integrat_calls.get()
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
            'error_code: -1, reason: Expecting POST, not GET')
