"""Tests from vumi.middleware.address_translator."""

from vumi.middleware.address_translator import AddressTranslationMiddleware
from vumi.tests.helpers import VumiTestCase


class TestAddressTranslationMiddleware(VumiTestCase):

    def mk_addr_trans(self, outbound_map):
        worker = object()
        config = {'outbound_map': outbound_map}
        mw = AddressTranslationMiddleware("test_addr_trans", config, worker)
        mw.setup_middleware()
        return mw

    def mk_msg(self, to_addr='unknown', from_addr='unknown'):
        return {
            'to_addr': to_addr,
            'from_addr': from_addr,
        }

    def test_handle_outbound(self):
        mw = self.mk_addr_trans({'555OUT': '555IN'})

        msg = mw.handle_outbound(self.mk_msg(to_addr="555OUT"), "outbound")
        self.assertEqual(msg['to_addr'], "555IN")
        msg = mw.handle_outbound(self.mk_msg(to_addr="555UNK"), "outbound")
        self.assertEqual(msg['to_addr'], "555UNK")

    def test_handle_inbound(self):
        mw = self.mk_addr_trans({'555OUT': '555IN'})

        msg = mw.handle_inbound(self.mk_msg(from_addr="555IN"), "inbound")
        self.assertEqual(msg['from_addr'], "555OUT")
        msg = mw.handle_inbound(self.mk_msg(from_addr="555UNK"), "inbound")
        self.assertEqual(msg['from_addr'], "555UNK")
