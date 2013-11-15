"""Tests for vumi.middleware.tagger."""

import re

from vumi.middleware.tagger import TaggingMiddleware
from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase


class TestTaggingMiddleware(VumiTestCase):

    DEFAULT_CONFIG = {
        'incoming': {
            'addr_pattern': r'^\d+(\d{3})$',
            'tagpool_template': r'pool1',
            'tagname_template': r'mytag-\1',
            },
        'outgoing': {
            'tagname_pattern': r'mytag-(\d{3})$',
            'msg_template': {
                'from_addr': r'1234*\1',
                },
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
                                   transport_name="dummy_connector",
                                   transport_type="dummy_transport_type")
        if tag is not None:
            TaggingMiddleware.add_tag_to_msg(msg, tag)
        return msg

    def get_tag(self, to_addr):
        msg = self.mk_msg(to_addr)
        msg = self.mw.handle_inbound(msg, "dummy_connector")
        return TaggingMiddleware.map_msg_to_tag(msg)

    def get_from_addr(self, to_addr, tag):
        msg = self.mk_msg(to_addr, tag, from_addr=None)
        msg = self.mw.handle_outbound(msg, "dummy_connector")
        return msg['from_addr']

    def test_inbound_matching_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag("123456"), ("pool1", "mytag-456"))
        self.assertEqual(self.get_tag("1234"), ("pool1", "mytag-234"))

    def test_inbound_nonmatching_to_addr(self):
        self.mk_tagger()
        self.assertEqual(self.get_tag("a1234"), None)

    def test_inbound_nonmatching_to_addr_leaves_msg_unmodified(self):
        self.mk_tagger()
        tag = ("dont", "modify")
        orig_msg = self.mk_msg("a1234", tag=tag)
        msg = orig_msg.from_json(orig_msg.to_json())
        msg = self.mw.handle_inbound(msg, "dummy_connector")
        self.assertEqual(msg, orig_msg)

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

    def test_outbound_nonmatching_tag_leaves_msg_unmodified(self):
        self.mk_tagger()
        orig_msg = self.mk_msg("a1234", tag=("pool1", "othertag-456"))
        msg = orig_msg.from_json(orig_msg.to_json())
        msg = self.mw.handle_outbound(msg, "dummy_connector")
        for key in msg.payload.keys():
            self.assertEqual(msg[key], orig_msg[key], "Key %r not equal" % key)
        self.assertEqual(msg, orig_msg)

    def test_outbound_no_tag(self):
        self.mk_tagger()
        self.assertEqual(self.get_from_addr("111", None), None)

    def test_deepupdate(self):
        orig = {'a': {'b': "foo"}, 'c': "bar"}
        TaggingMiddleware._deepupdate(re.match(".*", "foo"), orig,
                                      {'a': {'b': "baz"}, 'd': r'\g<0>!',
                                       'e': 1})
        self.assertEqual(orig, {'a': {'b': "baz"}, 'c': "bar", 'd': "foo!",
                                'e': 1})

    def test_deepupdate_with_recursion(self):
        self.mk_tagger()
        orig = {'a': {'b': "foo"}, 'c': "bar"}
        new = {'a': {'b': "baz"}}
        new['a']['d'] = new
        TaggingMiddleware._deepupdate(re.match(".*", "foo"), orig, new)
        self.assertEqual(orig, {'a': {'b': "baz"}, 'c': "bar"})

    def test_map_msg_to_tag(self):
        msg = self.mk_msg("123456")
        self.assertEqual(TaggingMiddleware.map_msg_to_tag(msg), None)
        msg['helper_metadata']['tag'] = {'tag': ['pool', 'mytag']}
        self.assertEqual(TaggingMiddleware.map_msg_to_tag(msg),
                         ("pool", "mytag"))

    def test_add_tag_to_msg(self):
        msg = self.mk_msg("123456")
        TaggingMiddleware.add_tag_to_msg(msg, ('pool', 'mytag'))
        self.assertEqual(msg['helper_metadata']['tag'], {
            'tag': ['pool', 'mytag'],
            })

    def test_add_tag_to_payload(self):
        payload = {}
        TaggingMiddleware.add_tag_to_payload(payload, ('pool', 'mytag'))
        self.assertEqual(payload, {
            'helper_metadata': {
                'tag': {
                    'tag': ['pool', 'mytag'],
                    },
                },
            })
