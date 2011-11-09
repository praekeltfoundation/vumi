"""Test for vumi.transport.infobip.infobip."""

import json

from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.utils import http_request
from vumi.transports.infobip.infobip import InfobipTransport
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class TestInfobipUssdTransport(TestCase):

    @inlineCallbacks
    def setUp(self):
        config = {
            'transport_name': 'test_infobip',
            'transport_type': 'ussd',
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
    def make_request(self, url_suffix, json_dict, reply=None,
                     continue_session=True):
        deferred_req = http_request(self.worker_url + url_suffix,
                                    json.dumps(json_dict), method='POST')
        [msg] = yield self.broker.wait_messages("vumi",
                                                "test_infobip.inbound", 1)
        msg = TransportUserMessage(**msg.payload)

        if reply is not None:
            reply_msg = msg.reply(reply, continue_session=continue_session)
            self.broker.publish_message("vumi", "test_infobip.outbound",
                                        reply_msg)

        response = yield deferred_req
        returnValue((msg, response))

    @inlineCallbacks
    def test_start(self):
        msg, response = yield self.make_request(
            "session/1/start",
            {
                'msisdn': '55567890',
                'text': "hello there",
                'shortCode': "*120*666#",
            },
            "hello yourself",
            )

        self.assertEqual(msg['content'], 'hello there')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_NEW)

        correct_response = {
            "shouldClose": False,
            "responseExitCode": 200,
            "ussdMenu": "hello yourself",
            "responseMessage": "",
            }
        self.assertEqual(json.loads(response), correct_response)

    @inlineCallbacks
    def test_response_with_close(self):
        msg, response = yield self.make_request(
            "session/1/response",
            {
                'msisdn': '55567890',
                'text': "More?",
                'shortCode': "*120*666#",
            },
            "No thanks.",
            continue_session=False,
            )

        self.assertEqual(msg['content'], 'More?')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_RESUME)

        correct_response = {
            "shouldClose": True,
            "responseExitCode": 200,
            "ussdMenu": "No thanks.",
            "responseMessage": "",
            }
        self.assertEqual(json.loads(response), correct_response)

    @inlineCallbacks
    def test_end(self):
        msg, response = yield self.make_request(
            "session/1/end",
            {
                'msisdn': '55567890',
                'text': 'Bye!',
                'shortCode': '*120*666#',
            },
            )

        self.assertEqual(msg['content'], 'Bye!')
        self.assertEqual(msg['session_event'],
                         TransportUserMessage.SESSION_CLOSE)

        correct_response = {
            "responseExitCode": 200,
            "responseMessage": "",
            }
        self.assertEqual(json.loads(response), correct_response)

    @inlineCallbacks
    def test_bad_request(self):
        num_tests = 2  # repeat twice to ensure transport is still functional
        json_dict = {
            'text': 'Oops. No msisdn.',
            }
        for _test in range(num_tests):
            response = yield http_request(self.worker_url + "session/1/start",
                                          json.dumps(json_dict), method='POST')
            self.assertTrue('exceptions.KeyError' in response)

        errors = self.flushLoggedErrors(KeyError)
        self.assertEqual([str(e.value) for e in errors],
                         ["'msisdn'"] * num_tests)
