"""Tests for vumi.middleware.tagger."""

from twisted.trial.unittest import TestCase

from vumi.middleware.tagger import TaggingMiddleware
from vumi.message import TransportUserMessage


class TaggingMiddlewareTestCase(TestCase):

    DEFAULT_CONFIG = {
        'addr_pattern': r'^\d+(\d{3})$',
        'tag_template': [r'pool1', r'mytag-\1'],
        }

    def mk_tagger(self, config=None):
        dummy_worker = object()
        if config is None:
            config = self.DEFAULT_CONFIG
        self.mw = TaggingMiddleware("dummy_tagger", config, dummy_worker)
        self.mw.setup_middleware()

    def get_tag(self, to_addr):
        msg = TransportUserMessage(to_addr=to_addr, from_addr="12345",
                                   transport_name="dummy_endpoint",
                                   transport_type="dummy_transport_type")
        msg = self.mw.handle_inbound(msg, "dummy_endpoint")
        return msg['tag']

    def test_matching_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag("123456"), ("pool1", "mytag-456"))
        self.assertEqual(self.get_tag("1234"), ("pool1", "mytag-234"))

    def test_nonmatching_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag("a1234"), None)
