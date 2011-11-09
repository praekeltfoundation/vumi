import re
import json
from xml.etree import ElementTree

from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.httprpc import HttpRpcTransport
from vumi.transports.tests.test_base import TransportTestCase
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class OkTransport(HttpRpcTransport):

    def handle_raw_inbound_message(self, msgid, request):
        self.publish_message(
                message_id=msgid,
                content='',
                to_addr='',
                from_addr='',
                provider='',
                session_event=TransportUserMessage.SESSION_NEW,
                transport_name=self.transport_name,
                transport_type=self.config.get('transport_type'),
                transport_metadata={},
                )


class TestHttpRPCTransport(TransportTestCase):

    transport_name = 'test_ok_transport'
    transport_class = OkTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestHttpRPCTransport, self).setUp()
        config = {
            'transport_name': self.transport_name,
            'transport_type': 'ok',
            'ussd_string_prefix': '',
            'web_path': "foo",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.transport = yield self.get_transport(config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {
            'pending_requests': 0
        })

    @inlineCallbacks
    def test_inbound(self):
        d = http_request(self.transport_url + "foo", '', method='GET')
        [msg] = yield self.wait_for_dispatched_messages(1)
        payload = msg.payload
        tum = TransportUserMessage(**payload)
        rep = tum.reply("OK")
        self.dispatch(rep)
        response = yield d
        self.assertEqual(response, 'OK')
