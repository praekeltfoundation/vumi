"""Test for vumi.transport.infobip.infobip."""

import json

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks
from twisted.internet.base import DelayedCall

from vumi.utils import http_request
from vumi.transports.infobip.infobip import InfobipTransport
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class TestInfobipUssdTransport(TestCase):

    @inlineCallbacks
    def setUp(self):
        DelayedCall.debug = True
        config = {
            'transport_name': 'test_infobip',
            'transport_type': 'ussd',
            'ussd_string_prefix': '*120*666',
            'web_path': "/session/",
            'web_port': 0,
            }
        self.worker = get_stubbed_worker(InfobipTransport, config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()
        addr = self.worker.web_resource.getHost()
        self.worker_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()

    @inlineCallbacks
    def test_start(self):
        json_content = json.dumps({
                'msisdn': '55567890',
                'text': "hello there",
                'shortCode': "*120*666#"
                })
        d = http_request(self.worker_url + "session/1/start",
                json_content, method='POST')
        msg, = yield self.broker.wait_messages("vumi",
            "test_infobip.inbound", 1)
        payload = msg.payload
        self.assertEqual(payload['content'], 'hello there')
        tum = TransportUserMessage(**payload)
        rep = tum.reply("hello yourself")
        self.broker.publish_message("vumi", "test_infobip.outbound",
                rep)
        response = yield d
        correct_response = {
                "shouldClose": False,
                "responseExitCode": 200,
                "ussdMenu": "hello yourself",
                "responseMessage": "",
                }
        self.assertEqual(json.loads(response), correct_response)
