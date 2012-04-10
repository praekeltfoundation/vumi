"""Tests for vumi.middleware.tagger."""

from twisted.trial.unittest import TestCase

from vumi.middleware.tagger import TaggingMiddleware
from vumi.message import TransportUserMessage


class TaggingMiddlewareTestCase(TestCase):

    DEFAULT_CONFIG = {
        'addr_pattern': r'^\d+(\d{3})$',
        'tag_template': [r'pool1', r'mytag-\1'],
        'tag_pattern': r'mytag-(\d{3})$',
        'msg_template': {
            'from_addr': r'1234*\1',
            },
        }

    def mk_tagger(self, config=None):
        dummy_worker = object()
        if config is None:
            config = self.DEFAULT_CONFIG
        self.mw = TaggingMiddleware("dummy_tagger", config, dummy_worker)
        self.mw.setup_middleware()

    def mk_msg(self, to_addr, tag=None, from_addr="12345"):
        msg = TransportUserMessage(to_addr=to_addr, from_addr=from_addr,
                                   transport_name="dummy_endpoint",
                                   transport_type="dummy_transport_type")
        if tag is not None:
            msg['tag'] = tag
        return msg

    def get_tag(self, to_addr):
        msg = self.mk_msg(to_addr)
        msg = self.mw.handle_inbound(msg, "dummy_endpoint")
        return msg['tag']

    def get_from_addr(self, to_addr, tag):
        msg = self.mk_msg(to_addr, tag, from_addr=None)
        msg = self.mw.handle_outbound(msg, "dummy_endpoint")
        return msg['from_addr']

    def test_inbound_matching_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag("123456"), ("pool1", "mytag-456"))
        self.assertEqual(self.get_tag("1234"), ("pool1", "mytag-234"))

    def test_inbound_nonmatching_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag("a1234"), None)

    def test_inbound_none_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag(None), None)

    def test_outbound_matching_tag(self):
        self.mk_tagger()
        self.assertEqual(self.get_from_addr("111", ("pool1", "mytag-456")),
                         "1234*456")
        self.assertEqual(self.get_from_addr("111", ("pool1", "mytag-789")),
                         "1234*789")

    def test_outbound_nonmatching_tag(self):
        self.mk_tagger()
        self.assertEqual(self.get_from_addr("111", ("pool1", "othertag-456")),
                         None)

    def test_outbound_no_tag(self):
        self.mk_tagger()
        self.assertEqual(self.get_from_addr("111", None), None)

    def test_map_msg_to_tag(self):
        msg = self.mk_msg("123456")
        self.assertEqual(TaggingMiddleware.map_msg_to_tag(msg), None)
        msg["tag"] = ["pool", "mytag"]
        self.assertEqual(TaggingMiddleware.map_msg_to_tag(msg),
                         ("pool", "mytag"))
