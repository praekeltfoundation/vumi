"""Test for vumi.transport.infobip.infobip_ussd."""

import json

from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.infobip.infobip_ussd import InfobipUssdTransport
from vumi.transports.tests.test_base import TransportTestCase
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class TestInfobipUssdTransport(TransportTestCase):

    timeout = 3
    transport_name = 'test_infobip'
    transport_class = InfobipUssdTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TestInfobipUssdTransport, self).setUp()
        config = {
            'transport_name': self.transport_name,
            'transport_type': 'ussd',
            'ussd_string_prefix': '*120*666',
            'web_path': "/session/",
            'web_host': "localhost",
            'web_port': 0,
            'username': 'testuser',
            'password': 'testpass',
            }
        self.transport = yield self.get_transport(config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def test_start(self):
        json_content = json.dumps({
            'msisdn': '55567890',
            'text':"hello there",
            'shortCode': "*120*666#"
        })

        d = http_request(self.transport_url + "session/1/start",
                json_content, method='POST')
        msg, = yield self.wait_for_dispatched_messages(1)
        payload = msg.payload
        self.assertEqual(payload['content'], 'hello there')
        tum = TransportUserMessage(**payload)
        rep = tum.reply("hello yourself")
        self.dispatch(rep)
        response = yield d
        correct_response = {
            "shouldClose": False,
            "responseExitCode": 200,
            "ussdMenu": "hello yourself",
            "responseMessage": "",
        }
        self.assertEqual(json.loads(response), correct_response)
