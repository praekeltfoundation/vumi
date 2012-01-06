"""Test for vumi.transport.truteq.truteq."""

from twisted.internet.defer import inlineCallbacks

from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.truteq.truteq import TruteqTransport
from vumi.tests.utils import LogCatcher


class TestTruteqTransport(TransportTestCase):

    transport_name = 'test_truteq_transport'
    transport_class = TruteqTransport

    @inlineCallbacks
    def setUp(self):
        super(TestTruteqTransport, self).setUp()
        self.config = {
            'username': 'vumitest',
            'password': 'notarealpassword',
            'host': 'localhost',
            'port': 1234,
            }
        self.transport = yield self.get_transport(self.config, start=True)

    def tearDown(self):
        super(TestTruteqTransport, self).tearDown()

    def test_handle_inbound_message(self):
        self.fail("Unimplemented test.")

    def test_handle_outbout_message(self):
        self.fail("Unimplemented test.")
