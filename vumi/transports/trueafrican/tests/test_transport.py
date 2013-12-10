from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock
from twisted.internet.error import ConnectionLost
from twisted.web.xmlrpc import Proxy

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.tests.utils import LogCatcher
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.trueafrican.transport import TrueAfricanUssdTransport


class TestTrueAfricanUssdTransport(VumiTestCase):

    SESSION_INIT_BODY = {
        'session': '1',
        'msisdn': '+27724385170',
        'shortcode': '*23#'
    }

    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(
            TransportHelper(TrueAfricanUssdTransport))
        self.clock = Clock()
        self.patch(TrueAfricanUssdTransport, 'get_clock', lambda _: self.clock)
        self.transport = yield self.tx_helper.get_transport({
            'interface': '127.0.0.1',
            'port': 0,
            'request_timeout': 10,
        })
        self.service_url = self.get_service_url(self.transport)

    def get_service_url(self, transport):
        """
        Get the URL for the HTTP resource. Requires the worker to be started.
        """
        addr = transport.web_resource.getHost()
        return "http://%s:%s/" % (addr.host, addr.port)

    def web_client(self):
        return Proxy(self.service_url)

    @inlineCallbacks
    def test_session_new(self):
        client = self.web_client()
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, "Oh Hai!")

        # verify the transport -> application message
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
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

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, "pong")

        yield resp_d
        yield self.tx_helper.clear_dispatched_inbound()

        # resume session
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'pong'}
        )

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, "ping")

        # verify the dispatched inbound message
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
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

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, "ping")

        yield resp_d
        yield self.tx_helper.clear_dispatched_inbound()

        # user initiated session termination
        resp_d = client.callRemote(
            'USSD.END',
            {'session': '1'}
        )

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
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
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.tx_helper.make_dispatch_reply(msg, "ping")
        yield resp_d
        yield self.tx_helper.clear_dispatched_inbound()

        # end session
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'o rly?'}
        )

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(
            msg, "kthxbye", continue_session=False)

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
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        rep = yield self.tx_helper.make_dispatch_reply(msg, "ping")
        yield resp_d

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(ack['event_type'], 'ack')
        self.assertEqual(ack['user_message_id'], rep['message_id'])
        self.assertEqual(ack['sent_message_id'], rep['message_id'])

    @inlineCallbacks
    def test_nack_for_outbound_message(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        # cancel the request and mute the resulting error.
        request = self.transport._requests[msg['message_id']]
        request.http_request.connectionLost(ConnectionLost())
        resp_d.cancel()
        resp_d.addErrback(lambda f: None)

        # send response
        rep = yield self.tx_helper.make_dispatch_reply(msg, "ping")
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], rep['message_id'])
        self.assertEqual(nack['sent_message_id'], rep['message_id'])

    @inlineCallbacks
    def test_nack_for_request_timeout(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.clock.advance(10.1)  # .1 second after timeout

        rep = yield self.tx_helper.make_dispatch_reply(msg, "ping")
        yield resp_d

        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['event_type'], 'nack')
        self.assertEqual(nack['user_message_id'], rep['message_id'])
        self.assertEqual(nack['sent_message_id'], rep['message_id'])
        self.assertEqual(nack['nack_reason'], 'Exceeded request timeout')

    @inlineCallbacks
    def test_nack_for_invalid_outbound_message(self):
        msg = yield self.tx_helper.make_dispatch_outbound("outbound")
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'],
                         'Missing in_reply_to, content or session_id fields')

    @inlineCallbacks
    def test_timeout(self):
        client = self.web_client()

        # initiate session
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
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

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, "pong")
        self.assertTrue(msg['message_id'] in self.transport._requests)
        yield resp_d
        self.assertFalse(msg['message_id'] in self.transport._requests)

    @inlineCallbacks
    def test_missing_session(self):
        """
        Verify that the transport handles missing session data in a
        graceful manner
        """
        client = self.web_client()
        resp_d = client.callRemote('USSD.INIT', self.SESSION_INIT_BODY)

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.tx_helper.make_dispatch_reply(msg, "pong")
        yield resp_d
        yield self.tx_helper.clear_dispatched_inbound()

        # simulate Redis falling over
        yield self.transport.session_manager.redis._purge_all()

        # resume
        resp_d = client.callRemote(
            'USSD.CONT',
            {'session': '1',
             'response': 'o rly?'}
        )

        resp = yield resp_d
        self.assertEqual(
            resp,
            {
                'message': ('We encountered an error while processing'
                            ' your message'),
                'type': 'end'
            }
        )
