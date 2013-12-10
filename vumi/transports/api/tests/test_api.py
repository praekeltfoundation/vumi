# -*- encoding: utf-8 -*-

import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request, http_request_full
from vumi.tests.helpers import VumiTestCase
from vumi.transports.api import HttpApiTransport
from vumi.transports.tests.helpers import TransportHelper


def config_override(**config):
    def deco(fun):
        fun.config_override = config
        return fun
    return deco


class TestHttpApiTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'web_path': "foo",
            'web_port': 0,
        }
        test_method = getattr(self, self._testMethodName)
        config_override = getattr(test_method, 'config_override', {})
        self.config.update(config_override)
        self.tx_helper = self.add_helper(TransportHelper(HttpApiTransport))

        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    def mkurl(self, content, from_addr=123, to_addr=555, **kw):
        params = {
            'to_addr': to_addr,
            'from_addr': from_addr,
            'content': content,
            }
        params.update(kw)
        return self.mkurl_raw(**params)

    def mkurl_raw(self, **params):
        return '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode(params)
        )

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(
            self.transport_url + "health", "", method='GET')
        self.assertEqual(json.loads(result), {'pending_requests': 0})

    @inlineCallbacks
    def test_inbound(self):
        url = self.mkurl('hello')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_handle_non_ascii_input(self):
        url = self.mkurl(u"öæł".encode("utf-8"))
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], u"öæł")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    @config_override(reply_expected=True)
    def test_inbound_with_reply(self):
        d = http_request(self.mkurl('hello'), '', method='GET')
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.make_dispatch_reply(msg, "OK")
        response = yield d
        self.assertEqual(response, 'OK')

    @inlineCallbacks
    def test_good_optional_parameter(self):
        url = self.mkurl('hello', group='#channel')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['group'], '#channel')
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    def test_bad_parameter(self):
        url = self.mkurl('hello', foo='bar')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'unexpected_parameter': ['foo']})

    @inlineCallbacks
    def test_missing_parameters(self):
        url = self.mkurl_raw(content='hello')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'missing_parameter': ['to_addr', 'from_addr']})

    @inlineCallbacks
    @config_override(field_defaults={'to_addr': '555'})
    def test_default_parameters(self):
        url = self.mkurl_raw(content='hello', from_addr='123')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    @config_override(field_defaults={'to_addr': '555'},
                     allowed_fields=['content', 'from_addr'])
    def test_disallowed_default_parameters(self):
        url = self.mkurl_raw(content='hello', from_addr='123')
        response = yield http_request(url, '', method='GET')
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], "hello")
        self.assertEqual(json.loads(response),
                         {'message_id': msg['message_id']})

    @inlineCallbacks
    @config_override(allowed_fields=['content', 'from_addr'])
    def test_disallowed_parameters(self):
        url = self.mkurl('hello')
        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(400, response.code)
        self.assertEqual(json.loads(response.delivered_body),
                         {'unexpected_parameter': ['to_addr']})
