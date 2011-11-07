from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.trial.unittest import SkipTest
from twisted.python import log
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.cellulant import CellulantTransport
from vumi.transports.httprpc.tests.utils import MockHttpServer
from vumi.message import TransportUserMessage
from vumi.utils import http_request
from urllib import urlencode


class TestCellulantTransportTestCase(TransportTestCase):

    transport_class = CellulantTransport
    transport_name = 'test_cellulant'

    @inlineCallbacks
    def setUp(self):
        yield super(TestCellulantTransportTestCase, self).setUp()
        self.config = {
            'ussd_code': '*120*1#',
            'web_port': 0,
            'web_path': '/api/v1/ussd/cellulant/',
        }
        self.transport = yield self.get_transport(self.config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s%s" % (addr.host, addr.port,
            self.config['web_path'])

    def mk_request(self, **params):
        defaults = {
            'MSISDN': '27761234567',
            'INPUT': '',
            'opCode': 'BEG',
            'ABORT': '0',
            'sessionID': '1',
        }
        defaults.update(params)
        return http_request('%s?%s' % (self.transport_url,
            urlencode(defaults)), data='', method='GET')

    @inlineCallbacks
    def test_inbound_begin(self):
        deferred = self.mk_request()

        [msg] = yield self._amqp.wait_messages("vumi",
                    "%s.inbound" % self.transport_name, 1)
        self.assertEqual(msg['content'], '')
        self.assertEqual(msg['to_addr'], '*120*1#')
        self.assertEqual(msg['from_addr'], '27761234567'),
        self.assertEqual(msg['session_event'], TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'session_id': '1',
        })

        reply = TransportUserMessage(**msg.payload).reply("ussd message")
        self._amqp.publish_message("vumi", "%s.outbound" % self.transport_name,
                reply)
        response = yield deferred
        self.assertEqual(response, '1|ussd message|null|null|null|null')

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        deferred = self.mk_request(INPUT='hi', opCode='')

        [msg] = yield self._amqp.wait_messages("vumi",
            "%s.inbound" % self.transport_name, 1)
        self.assertEqual(msg['content'], 'hi')
        self.assertEqual(msg['to_addr'], '*120*1#')
        self.assertEqual(msg['from_addr'], '27761234567')
        self.assertEqual(msg['session_event'], TransportUserMessage.SESSION_RESUME)
        self.assertEqual(msg['transport_metadata'], {
            'session_id': '1',
        })

        reply = TransportUserMessage(**msg.payload).reply("hello world",
            continue_session=False)
        self._amqp.publish_message("vumi", "%s.outbound" % self.transport_name,
            reply)
        response = yield deferred
        self.assertEqual(response, '1|hello world|null|null|end|null')

    @inlineCallbacks
    def test_inbound_abort_opcode(self):
        # this one should return immediately with a blank
        # as there isn't going to be a sensible response
        resp = yield self.mk_request(opCode='ABO')
        self.assertEqual(resp, '')

        [msg] = yield self._amqp.wait_messages("vumi",
            "%s.inbound" % self.transport_name, 1)
        self.assertEqual(msg['session_event'], TransportUserMessage.SESSION_CLOSE)

    @inlineCallbacks
    def test_inbound_abort_field(self):
        # should also return immediately
        resp = yield self.mk_request(ABORT=1)
        self.assertEqual(resp, '')
        [msg] = yield self._amqp.wait_messages("vumi",
            "%s.inbound" % self.transport_name, 1)
        self.assertEqual(msg['session_event'], TransportUserMessage.SESSION_CLOSE)
