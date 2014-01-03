from twisted.internet.defer import inlineCallbacks, succeed
from txtwitter.twitter import TwitterClient

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.twitter import TwitterTransport
from vumi.transports.tests.helpers import TransportHelper


class FakeTwitterStreamService(object):
    def __init__(self, delegate):
        self.delegate = delegate

    def startService(self):
        pass

    def stopService(self):
        pass

    def respond_with(self, data):
        self.delegate(data)


class FakeTwitterClient(TwitterClient):
    timeout = 1

    def __init__(self, *a, **kw):
        super(FakeTwitterClient, self).__init__(*a, **kw)
        self.stream_filters = []
        self.updates = []
        self.update_response = None
        self.update_error = None

    def set_update_response(self, resp):
        self.update_response = resp

    def set_update_to_fail(self, e):
        self.update_error = e

    def get_updates(self):
        return self.updates

    def update(self, content):
        self.updates.append(content)

        if self.update_error is not None:
            raise self.update_error

        return succeed(self.update_response)

    def stream_filter(self, delegate, track=None):
        return FakeTwitterStreamService(delegate)


class StubbedTwitterTransport(TwitterTransport):
    CLIENT_CLS = FakeTwitterClient


class TestTwitterTransport(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.tx_helper = self.add_helper(
            TransportHelper(StubbedTwitterTransport))

        self.transport = yield self.tx_helper.get_transport({
            'consumer_key': 'consumer1',
            'consumer_secret': 'consumersecret1',
            'access_token': 'token1',
            'access_token_secret': 'tokensecret1',
            'terms': ['some', 'trending', 'topic'],
        })

    @inlineCallbacks
    def test_handle_track(self):
        self.transport.track_stream.respond_with({
            'id_str': '1',
            'text': 'text',
            'in_reply_to_screen_name': '@reply_to',
            'user': {'screen_name': '@screen_name'},
        })

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)

        self.assertEqual(msg['from_addr'], '@screen_name')
        self.assertEqual(msg['to_addr'], '@reply_to')
        self.assertEqual(msg['content'], 'text')
        self.assertEqual(msg['message_id'], '1')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NONE)

    @inlineCallbacks
    def test_sending(self):
        self.transport.client.set_update_response({'id_str': '1'})

        msg = yield self.tx_helper.make_dispatch_outbound('adnap das')
        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(self.transport.client.get_updates(), ['adnap das'])
        self.assertEqual(ack['user_message_id'], msg['message_id'])
        self.assertEqual(ack['sent_message_id'], '1')

    @inlineCallbacks
    def test_sending_failure(self):
        error = Exception(':(')
        self.transport.client.set_update_response({'id_str': '1'})
        self.transport.client.set_update_to_fail(error)

        msg = yield self.tx_helper.make_dispatch_outbound('adnap das')
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)

        self.assertEqual(self.transport.client.get_updates(), ['adnap das'])
        self.assertEqual(nack['user_message_id'], msg['message_id'])
        self.assertEqual(nack['sent_message_id'], msg['message_id'])
        self.assertEqual(nack['nack_reason'], '%r' % (error,))
