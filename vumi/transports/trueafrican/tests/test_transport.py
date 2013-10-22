from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet import reactor
from twisted.web.xmlrpc import Proxy

from vumi.transports.tests.utils import TransportTestCase
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
        addr = transport.web_resource.getHost()
        return "http://%s:%s/" % (addr.host, addr.port)

    def service_client(self):
        return Proxy(self.service_url)

    def reply_to_message(self, *args, **kw):
        def reply(r):
            msg = TransportUserMessage(**r[0].payload)
            self.dispatch(msg.reply(*args, **kw))
            return msg
        d = self.wait_for_dispatched_messages(1)
        return d.addCallback(reply)

    @inlineCallbacks
    def test_session_init(self):
        client = self.service_client()
        resp_d = client.callRemote(
            'USSD.INIT',
            {'session': '1',
             'msisdn': '+27724385170',
             'shortcode': '*23#'}
        )
        msg = yield self.reply_to_message("Oh Hai!")

        # verify the transport -> application message
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['from_addr'], '+27724385170')
        self.assertEqual(msg['to_addr'], '*23#')
        self.assertEqual(msg['content'], None)

        resp = yield resp_d
        self.assertEqual(
            resp,
            {
                'message': 'Oh Hai!',
                'session': '1',
                'status': 'OK',
                'type': 'cont'
            }
        )

    @inlineCallbacks
    def test_session_resume(self):
        client = self.service_client()

        # initiate session
        resp_d = client.callRemote(
            'USSD.INIT',
            {'session': '1',
             'msisdn': '+27724385170',
             'shortcode': '*23#'}
        )
        yield self.reply_to_message("ping")
        yield resp_d
        yield self.clear_dispatched_messages()

        # resume session
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'pong'}
        )
        msg = yield self.reply_to_message("ping")
        # verify the transport -> application message
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['from_addr'], '+27724385170')
        self.assertEqual(msg['to_addr'], '*23#')
        self.assertEqual(msg['content'], 'pong')

        resp = yield resp_d
        self.assertEqual(
            resp,
            {
                'message': 'ping',
                'session': '1',
                'status': 'OK',
                'type': 'cont'
            }
        )

    @inlineCallbacks
    def test_session_end_user_initiated(self):
        client = self.service_client()

        # initiate session
        resp_d = client.callRemote(
            'USSD.INIT',
            {'session': '1',
             'msisdn': '+27724385170',
             'shortcode': '*23#'}
        )
        yield self.reply_to_message("ping")
        yield resp_d
        yield self.clear_dispatched_messages()

        # user initiated session termination
        resp_d = client.callRemote(
            'USSD.END',
            {'session': '1'}
        )

        [msg] = yield self.wait_for_dispatched_messages(1)
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(msg['from_addr'], '+27724385170')
        self.assertEqual(msg['to_addr'], '*23#')
        self.assertEqual(msg['content'], None)

        resp = yield resp_d
        self.assertEqual(
            resp,
            {
                'status': 'OK',
            }
        )

    @inlineCallbacks
    def test_session_end_application_initiated(self):
        client = self.service_client()

        # initiate session
        resp_d = client.callRemote(
            'USSD.INIT',
            {'session': '1',
             'msisdn': '+27724385170',
             'shortcode': '*23#'}
        )
        yield self.reply_to_message("ping")
        yield resp_d
        yield self.clear_dispatched_messages()

        # end session
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'o rly?'}
        )
        yield self.reply_to_message(
            "kthxbye",
            session_event=TransportUserMessage.SESSION_CLOSE
        )

        resp = yield resp_d
        self.assertEqual(
            resp,
            {
                'message': 'kthxbye',
                'session': '1',
                'status': 'OK',
                'type': 'end'
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
                         'Missing in_reply_to, content or session_id fields')
