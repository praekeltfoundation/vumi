from twisted.internet.defer import inlineCallbacks
from twisted.web.xmlrpc import Proxy

from vumi.transports.tests.utils import TransportTestCase
from vumi.utils import http_request_full
from vumi.message import TransportUserMessage
from vumi.transports.trueafrican.transport import TrueAfricanUssdTransport


class TestTrueAfricanUssdTransport(TransportTestCase):

    timeout = 1
    transport_name = 'trueafrican_ussd'
    transport_class = TrueAfricanUssdTransport

    @inlineCallbacks
    def setUp(self):
        yield super(TrueAfricanUssdTransport, self).setUp()
        self.config = {
            'server_hostname': '127.0.0.1',
            'server_port': 0,
        }
        self.transport = yield self.get_transport(self.config)
        self.client = Proxy('127.0.0.1:

        def make_request(self):
