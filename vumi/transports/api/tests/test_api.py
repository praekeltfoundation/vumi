import json
from urllib import urlencode
from twisted.trial.unittest import TestCase
from twisted.web.resource import Resource
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.web.server import Site
from twisted.internet import reactor

from vumi.utils import http_request
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.api import (HttpApiTransport,
                                OldSimpleHttpTransport,
                                OldTemplateHttpTransport)
from vumi.message import TransportUserMessage
from vumi.tests.utils import get_stubbed_worker


class TestHttpApiTransport(TransportTestCase):

    transport_name = 'test_http_api_transport'
    transport_class = HttpApiTransport

    @inlineCallbacks
    def setUp(self):
        super(TestHttpApiTransport, self).setUp()
        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
        }

        self.transport = yield self.get_transport(self.config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {})

    @inlineCallbacks
    def test_inbound(self):
        url = '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode({
                'to_addr': 555,
                'from_addr': 123,
                'content': 'hello',
            })
        )
        response = yield http_request(url, '', method='GET')
        [msg] = self.get_dispatched_messages()
        payload = msg.payload
        self.assertEqual(payload['transport_name'], self.transport_name)
        self.assertEqual(payload['to_addr'], "555")
        self.assertEqual(payload['from_addr'], "123")
        self.assertEqual(payload['content'], "hello")
        self.assertEqual(json.loads(response), {
            'message_id': payload['message_id'],
        })



class TestOldSimpleHttpTransport(TransportTestCase):

    transport_name = 'test_old_simple_http_transport'
    transport_class = OldSimpleHttpTransport

    @inlineCallbacks
    def setUp(self):
        super(TestOldSimpleHttpTransport, self).setUp()
        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
        }

        self.transport = yield self.get_transport(self.config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def test_health(self):
        result = yield http_request(self.transport_url + "health", "",
                                    method='GET')
        self.assertEqual(json.loads(result), {})

    @inlineCallbacks
    def test_inbound(self):
        url = '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode([
                ('to_msisdn', 555),
                ('to_msisdn', 556),
                ('from_msisdn', 123),
                ('message', 'hello'),
            ])
        )
        response = yield http_request(url, '', method='GET')
        [msg1, msg2] = self.get_dispatched_messages()
        payload1 = msg1.payload
        payload2 = msg2.payload
        self.assertEqual(payload1['transport_name'], self.transport_name)
        self.assertEqual(payload1['to_addr'], "555")
        self.assertEqual(payload2['to_addr'], "556")
        self.assertEqual(payload1['from_addr'], "123")
        self.assertEqual(payload1['content'], "hello")
        self.assertEqual(json.loads(response), [
            {
                'id': payload1['message_id'],
                'message': payload1['content'],
                'from_msisdn': payload1['from_addr'],
                'to_msisdn': payload1['to_addr'],
            },
            {
                'id': payload2['message_id'],
                'message': payload2['content'],
                'from_msisdn': payload2['from_addr'],
                'to_msisdn': payload2['to_addr'],
            },
            ])


class TestOldTemplateHttpTransport(TestOldSimpleHttpTransport):

    transport_name = 'test_old_template_http_transport'
    transport_class = OldTemplateHttpTransport

    @inlineCallbacks
    def setUp(self):
        super(TestOldTemplateHttpTransport, self).setUp()
        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
        }

        self.transport = yield self.get_transport(self.config)
        addr = self.transport.web_resource.getHost()
        self.transport_url = "http://%s:%s/" % (addr.host, addr.port)

    @inlineCallbacks
    def test_inbound(self):
        url = '%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            urlencode([
                ('to_msisdn', 555),
                ('to_msisdn', 556),
                ('template_name', "Joe"),
                ('template_name', "Foo"),
                ('template_surname', "Smith"),
                ('template_surname', "Bar"),
                ('from_msisdn', 123),
                ('template', 'hello {{ name }} {{surname}}'),
            ])
        )

        response = yield http_request(url, '', method='GET')
        [msg1, msg2] = self.get_dispatched_messages()
        payload1 = msg1.payload
        payload2 = msg2.payload
        self.assertEqual(payload1['transport_name'], self.transport_name)
        self.assertEqual(payload1['to_addr'], "555")
        self.assertEqual(payload1['from_addr'], "123")
        self.assertEqual(payload1['content'], "hello Joe Smith")
        self.assertEqual(payload2['content'], "hello Foo Bar")
        self.assertEqual(json.loads(response), [
            {
            'id': payload1['message_id'],
            'message': payload1['content'],
            'from_msisdn': payload1['from_addr'],
            'to_msisdn': payload1['to_addr'],
            },
            {
            'id': payload2['message_id'],
            'message': payload2['content'],
            'from_msisdn': payload2['from_addr'],
            'to_msisdn': payload2['to_addr'],
            },
            ])
