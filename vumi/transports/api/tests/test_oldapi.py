from base64 import b64encode
import json
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks
from twisted.web import http

from vumi.utils import http_request, http_request_full
from vumi.tests.helpers import VumiTestCase
from vumi.transports.api import (
    OldSimpleHttpTransport, OldTemplateHttpTransport)
from vumi.transports.tests.helpers import TransportHelper


class TestOldSimpleHttpTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'web_path': "foo",
            'web_port': 0,
        }
        self.tx_helper = self.add_helper(
            TransportHelper(OldSimpleHttpTransport))

        self.transport = yield self.tx_helper.get_transport(self.config)
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
        [msg1, msg2] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg1['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg1['to_addr'], "555")
        self.assertEqual(msg2['to_addr'], "556")
        self.assertEqual(msg1['from_addr'], "123")
        self.assertEqual(msg1['content'], "hello")
        self.assertEqual(json.loads(response), [
            {
                'id': msg1['message_id'],
                'message': msg1['content'],
                'from_msisdn': msg1['from_addr'],
                'to_msisdn': msg1['to_addr'],
            },
            {
                'id': msg2['message_id'],
                'message': msg2['content'],
                'from_msisdn': msg2['from_addr'],
                'to_msisdn': msg2['to_addr'],
            },
        ])

    @inlineCallbacks
    def test_http_basic_auth(self):
        http_auth_config = self.config.copy()
        http_auth_config.update({
            'identities': {
                'username': 'password',
            }
        })
        transport = yield self.tx_helper.get_transport(http_auth_config)
        url = '%s%s?%s' % (
            transport.get_transport_url(),
            self.config['web_path'],
            urlencode({
                'to_msisdn': '123',
                'from_msisdn': '456',
                'message': 'hello',
            }))

        response = yield http_request_full(url, '', method='GET')
        self.assertEqual(response.code, http.UNAUTHORIZED)
        self.assertEqual([], self.tx_helper.get_dispatched_inbound())

        response = yield http_request_full(url, '', headers={
            'Authorization': ['Basic %s' % b64encode('username:password')]
        }, method='GET')
        self.assertEqual(response.code, http.OK)
        [msg] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg['content'], 'hello')
        self.assertEqual(msg['transport_metadata'], {
            'http_user': 'username',
        })


class TestOldTemplateHttpTransport(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'web_path': "foo",
            'web_port': 0,
        }
        self.tx_helper = self.add_helper(
            TransportHelper(OldTemplateHttpTransport))

        self.transport = yield self.tx_helper.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

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
        [msg1, msg2] = self.tx_helper.get_dispatched_inbound()
        self.assertEqual(msg1['transport_name'], self.tx_helper.transport_name)
        self.assertEqual(msg1['to_addr'], "555")
        self.assertEqual(msg1['from_addr'], "123")
        self.assertEqual(msg1['content'], "hello Joe Smith")
        self.assertEqual(msg2['content'], "hello Foo Bar")
        self.assertEqual(json.loads(response), [
            {
                'id': msg1['message_id'],
                'message': msg1['content'],
                'from_msisdn': msg1['from_addr'],
                'to_msisdn': msg1['to_addr'],
            },
            {
                'id': msg2['message_id'],
                'message': msg2['content'],
                'from_msisdn': msg2['from_addr'],
                'to_msisdn': msg2['to_addr'],
            },
        ])
