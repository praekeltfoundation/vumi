from twisted.internet.defer import inlineCallbacks
from twisted.web.xmlrpc import Proxy

from vumi.transports.tests.utils import TransportTestCase
from vumi.utils import http_request_full
from vumi.message import TransportUserMessage
from vumi.transports.trueafrican.transport import TrueAfricanUssdTransport


class TestTrueAfricanUssdTransport(TransportTestCase):

    timeout = 1
    transport_name = 'trueafrican_ussd'
    transport_class = TrueAfricanUssdTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestTrueAfricanUssdTransport, self).setUp()
        self.config = {
            'server_hostname': '127.0.0.1',
            'server_port': 0,
        }
        self.transport = yield self.get_transport(self.config)
        self.service_url = self.get_service_url(self.transport)

    def get_service_url(self, transport):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.
        """
        addr = transport.web_service._getPort().getHost()
        return "http://%s:%s/" % (addr.host, addr.port)

    def service_client(self):
        return Proxy(self.service_url)

    def reply_to_message(self, *args, **kw):
        """
        Fake a response from an application worker
        """
        d = self.wait_for_dispatched_messages(1)

        def reply(r):
            msg = TransportUserMessage(**r[0].payload)
            self.dispatch(msg.reply(*args, **kw))
            return msg

        return d.addCallback(reply)

    @inlineCallbacks
    def test_session_init(self):
        client = self.service_client()
        resp_d = client.callRemote('USSD.INIT',
                                   {'session': '32423',
                                    'msisdn': '+27724385170',
                                    'shortcode': '*23#'})
        yield self.reply_to_message("Hello!")
        resp = yield resp_d
        self.assertEquals(
            resp,
            {
                'message': 'Hello!',
                'session': '32423',
                'type': 'cont'
            }
        )

    @inlineCallbacks
    def test_nack(self):
        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
                         'Missing in_reply_to, content or session_id')
