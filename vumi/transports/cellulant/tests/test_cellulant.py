from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import SkipTest
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.cellulant import CellulantTransport
from vumi.transports.httprpc.tests.utils import MockHttpServer
from vumi.utils import http_request


class TestCellulantTransportTestCase(TransportTestCase):

    transport_name = 'test_cellulant'

    @inlineCallbacks
    def setUp(self):
        yield super(TestCellulantTransportTestCase, self).setUp()
        self.transport = yield self.get_transport({
            'ussd_code': '*120*1#',
            'web_port': 0,
            'web_path': '/api/v1/ussd/cellulant/',
        }, CellulantTransport)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    def tearDown(self):
        super(TestCellulantTransportTestCase, self).tearDown()

    def test_inbound(self):
        deferred = http_request(self.transport_url, {
            'MSISDN': '27761234567',
            'INPUT': '',
            'opCode': 'BEG',
            'ABORT': '0',
            'sessionID': '1',
        }, method='GET')

        msg, = yield self.broker.wait_messages("vumi",
            "%s.inbound" % self.transport_name, 1)
        self.assertEqual(msg['content'], '')
        self.assertEqual(msg['to_addr'], '*120*1#')
        self.assertEqual(msg['from_addr'], '27761234567'),
        self.assertEqual(msg['session_event'], TransportUserMessage.SESSION_NEW)
        self.assertEqual(msg['transport_metadata'], {
            'session_id': '1',
        })

        rep = msg.reply("ussd message")
        self.broker.publish_message("vumi", "test_infobip.outbound",
                rep)
        response = yield deferred
        self.assertEqual(response, '1|ussd message|null|null|null|null')

    def test_outbound(self):
        raise SkipTest('not implemented yet')
