# -*- encoding: utf-8 -*-

"""Tests for SMSSync transport."""

import json
import datetime
from urllib import urlencode

from twisted.internet.defer import inlineCallbacks

from vumi.utils import http_request
from vumi.transports.tests.test_base import TransportTestCase
from vumi.transports.smssync import SingleSmsSync, MultiSmsSync


class TestSingleSmsSync(TransportTestCase):

    transport_name = 'test_smssync_transport'
    transport_class = SingleSmsSync
    multi_smssync = False

    @inlineCallbacks
    def setUp(self):
        super(TestSingleSmsSync, self).setUp()
        self.smssync_secret = "secretsecret"
        self.config = {
            'transport_name': self.transport_name,
            'web_path': "foo",
            'web_port': 0,
        }
        if not self.multi_smssync:
            self.config['smssync_secret'] = self.smssync_secret
        self.transport = yield self.get_transport(self.config)
        self.transport_url = self.transport.get_transport_url()

    def smssync_inbound(self, content, from_addr='123', to_addr='555',
                        timestamp=None, message_id='1', secret=None):
        """Emulate an inbound message from SMSSync on an Android phone."""
        if timestamp is None:
            timestamp = datetime.datetime.utcnow()
        if secret is None:
            secret = self.default_param_secret()
        # Timestamp format: mm-dd-yy-hh:mm, e.g. 11-27-11-07:11
        params = {
            'sent_to': to_addr,
            'from': from_addr,
            'message': content,
            'sent_timestamp': timestamp.strftime("%m-%d-%y-%H:%M"),
            'message_id': message_id,
            'secret': secret,
        }
        return self.smssync_call(params, method='POST')

    def smssync_poll(self):
        """Emulate a poll from SMSSync for waiting outbound messages."""
        return self.smssync_call({'task': 'send'}, method='GET')

    def smssync_call(self, params, method):
        url = self.mkurl(params)
        d = http_request(url, '', method=method)
        d.addCallback(json.loads)
        return d

    def mkurl(self, params):
        params = dict((k.encode('utf-8'), v.encode('utf-8'))
                      for k, v in params.items())
        return '%s%s%s?%s' % (
            self.transport_url,
            self.config['web_path'],
            self.default_url_secret(),
            urlencode(params),
        )

    def default_url_secret(self):
        return ''

    def default_param_secret(self):
        return self.smssync_secret

    def add_default_secret_key_to_msg(self, msg):
        pass

    @inlineCallbacks
    def test_inbound_success(self):
        now = datetime.datetime.utcnow().replace(second=0, microsecond=0)
        response = yield self.smssync_inbound(content=u'hællo', timestamp=now)
        self.assertEqual(response, {"payload": {"success": "true"}})

        [msg] = self.get_dispatched_messages()
        self.assertEqual(msg['transport_name'], self.transport_name)
        self.assertEqual(msg['to_addr'], "555")
        self.assertEqual(msg['from_addr'], "123")
        self.assertEqual(msg['content'], u"hællo")
        self.assertEqual(msg['timestamp'], now)

    @inlineCallbacks
    def test_inbound_invalid_secret(self):
        response = yield self.smssync_inbound(content=u'hello', secret='wrong')
        self.assertEqual(response, {"payload": {"success": "false"}})

    @inlineCallbacks
    def test_poll_outbound(self):
        outbound_msg = self.mkmsg_out(content=u'hællo')
        self.add_default_secret_key_to_msg(outbound_msg)
        yield self.dispatch(outbound_msg)
        response = yield self.smssync_poll()
        self.assertEqual(response, {
            "payload": {
                "task": "send",
                "secret": self.default_param_secret(),
                "messages": [{
                    "to": outbound_msg['to_addr'],
                    "message": outbound_msg['content'],
                },
                ],
            },
        })

    # TODO: test replies


class TestMultiSmsSync(TestSingleSmsSync):

    transport_name = 'test_multismssync_transport'
    transport_class = MultiSmsSync
    multi_smssync = True

    def default_url_secret(self):
        return "/" + self.smssync_secret + "/"

    def default_param_secret(self):
        return ""

    def add_default_secret_key_to_msg(self, msg):
        secret_key = self.transport.key_for_secret(self.smssync_secret)
        self.transport.add_secret_key_to_msg(msg, secret_key)
