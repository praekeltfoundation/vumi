from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock
from twisted.internet.error import ConnectionLost
from twisted.web.xmlrpc import Proxy

from vumi.transports.tests.utils import TransportTestCase
from vumi.message import TransportUserMessage
from vumi.transports.trueafrican.transport import TrueAfricanUssdTransport
from vumi.tests.utils import LogCatcher


class TestTrueAfricanUssdTransport(TransportTestCase):

    timeout = 1
    transport_name = 'trueafrican_ussd'
    transport_class = TrueAfricanUssdTransport

    SESSION_INIT_BODY = {
        'session': '1',
        'msisdn': '+27724385170',
        'shortcode': '*23#'
    }

    @inlineCallbacks
    def setUp(self):
        yield super(TestTrueAfricanUssdTransport, self).setUp()
        self.config = {
            'interface': '127.0.0.1',
            'port': 0,
            'request_timeout': 10,
        }
        self.clock = Clock()
        self.patch(TrueAfricanUssdTransport, 'get_clock', lambda _: self.clock)
        self.transport = yield self.get_transport(self.config)
        self.service_url = self.get_service_url(self.transport)

    def get_service_url(self, transport):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.
        """
        addr = transport.web_resource.getHost()
        return "http://%s:%s/" % (addr.host, addr.port)

    def web_client(self):
        return Proxy(self.service_url)

    def reply_to_message(self, *args, **kw):
        def reply(r):
            msg = TransportUserMessage(**r[0].payload)
            self.dispatch(msg.reply(*args, **kw))
            return msg
        d = self.wait_for_dispatched_messages(1)
        return d.addCallback(reply)

    def reply(self, msg, *args, **kw):
        msg_out = TransportUserMessage(**msg.payload).reply(*args, **kw)
        self.dispatch(msg_out)
        return msg_out

    @inlineCallbacks
    def test_session_new(self):
        client = self.web_client()
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.wait_for_dispatched_inbound(1)
        yield self.reply(msg, "Oh Hai!")

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
                'type': 'cont'
            }
        )

    @inlineCallbacks
    def test_session_resume(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.wait_for_dispatched_inbound(1)
        yield self.reply(msg, "pong")

        yield resp_d
        yield self.clear_dispatched_messages()

        # resume session
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'pong'}
        )

        [msg] = yield self.wait_for_dispatched_inbound(1)
        yield self.reply(msg, "ping")

        # verify the dispatched inbound message
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
                'type': 'cont'
            }
        )

    @inlineCallbacks
    def test_session_end_user_initiated(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.wait_for_dispatched_inbound(1)
        yield self.reply(msg, "ping")

        yield resp_d
        yield self.clear_dispatched_messages()

        # user initiated session termination
        resp_d = client.callRemote(
            'USSD.END',
            {'session': '1'}
        )

        [msg] = yield self.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['transport_type'], "ussd")
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)
        self.assertEqual(msg['from_addr'], '+27724385170')
        self.assertEqual(msg['to_addr'], '*23#')
        self.assertEqual(msg['content'], None)

        resp = yield resp_d
        self.assertEqual(resp, {})

    @inlineCallbacks
    def test_session_end_application_initiated(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)
        [msg] = yield self.wait_for_dispatched_inbound(1)

        yield self.reply(msg, "ping")
        yield resp_d
        yield self.clear_dispatched_messages()

        # end session
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'o rly?'}
        )

        [msg] = yield self.wait_for_dispatched_inbound(1)
        yield self.reply(msg, "kthxbye",
                         session_event=TransportUserMessage.SESSION_CLOSE)

        resp = yield resp_d
        self.assertEqual(
            resp,
            {
                'message': 'kthxbye',
                'session': '1',
                'type': 'end'
            }
        )

    @inlineCallbacks
    def test_ack_for_outbound_message(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        # send response
        [msg] = yield self.wait_for_dispatched_inbound(1)
        rep = yield self.reply(msg, "ping")
        yield resp_d

        [ack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], rep['message_id'])
        self.assertEqual(ack['sent_message_id'], rep['message_id'])

    @inlineCallbacks
    def test_nack_for_outbound_message(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.wait_for_dispatched_messages(1)

        # cancel the request and mute the resulting error.
        request = self.transport._requests[msg['message_id']]
        request.http_request.connectionLost(ConnectionLost())
        resp_d.cancel()
        resp_d.addErrback(lambda f: None)

        # send response
        tum = TransportUserMessage(**msg.payload)
        rep = tum.reply("ping")

        yield self.dispatch(rep)
        [nack] = yield self.wait_for_dispatched_events(1)

        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], rep['message_id'])
        self.assertEqual(nack['sent_message_id'], rep['message_id'])

    @inlineCallbacks
    def test_nack_for_invalid_outbound_message(self):
        msg = self.mkmsg_out()
        yield self.dispatch(msg)
        [nack] = yield self.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
                         'Missing in_reply_to, content or session_id fields')

    @inlineCallbacks
    def test_timeout(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.wait_for_dispatched_inbound(1)
        with LogCatcher(message='Timing out') as lc:
            self.assertTrue(msg['message_id'] in self.transport._requests)
            self.clock.advance(10.1)  # .1 second after timeout
            self.assertFalse(msg['message_id'] in self.transport._requests)
            [warning] = lc.messages()
            self.assertEqual(warning,
                             'Timing out on response for +27724385170')
            resp = yield resp_d
            self.assertEqual(
                resp,
                {
                    'message': ('We encountered an error while processing'
                                ' your message'),
                    'type': 'end'
                }
            )

    @inlineCallbacks
    def test_request_tracking(self):
        """
        Verify that the transport cleans up after finishing a request
        """
        client = self.web_client()
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.wait_for_dispatched_inbound(1)
        yield self.reply(msg, "pong")
        self.assertTrue(msg['message_id'] in self.transport._requests)
        yield resp_d
        self.assertFalse(msg['message_id'] in self.transport._requests)
