# -*- coding: utf-8 -*-

"""Tests for vumi.transports.dmark.dmark_ussd."""

import json

from twisted.internet.defer import inlineCallbacks

from vumi.message import TransportUserMessage
from vumi.tests.helpers import VumiTestCase
from vumi.transports.dmark import DmarkUssdTransport
from vumi.transports.httprpc.tests.helpers import HttpRpcTransportHelper


class TestDmarkUssdTransport(VumiTestCase):

    _transaction_id = u'transaction-123'
    _to_addr = '*121#'
    _from_addr = '+00775551122'
    _request_defaults = {
        'transactionId': _transaction_id,
        'msisdn': _from_addr,
        'ussdServiceCode': _to_addr,
        'transactionTime': '1389971940',
        'ussdRequestString': _to_addr,
        'creationTime': '1389971950',
        'response': 'false',
    }

    @inlineCallbacks
    def setUp(self):
        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/dmark/ussd/',
        }
        self.tx_helper = self.add_helper(
            HttpRpcTransportHelper(DmarkUssdTransport,
                                   request_defaults=self._request_defaults))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.session_manager = self.transport.session_manager
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])
        yield self.session_manager.redis._purge_all()  # just in case

    @inlineCallbacks
    def mk_session(self, transaction_id=_transaction_id):
        yield self.session_manager.create_session(
            transaction_id, transaction_id=transaction_id)

    def assert_inbound_message(self, msg, **field_values):
        expected_field_values = {
            'content': self._request_defaults['ussdRequestString'],
            'to_addr': self._to_addr,
            'from_addr': self._from_addr,
            'session_event': TransportUserMessage.SESSION_NEW,
            'transport_metadata': {
                'dmark_ussd': {
                    'transaction_id':
                        self._request_defaults['transactionId'],
                    'transaction_time':
                        self._request_defaults['transactionTime'],
                    'creation_time':
                        self._request_defaults['creationTime'],
                },
            }
        }
        expected_field_values.update(field_values)

        for field, expected_value in expected_field_values.iteritems():
            self.assertEqual(msg[field], expected_value)

    def assert_ack(self, ack, reply):
        self.assertEqual(ack.payload['event_type'], 'ack')
        self.assertEqual(ack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(ack.payload['sent_message_id'], reply['message_id'])

    def assert_nack(self, nack, reply, reason):
        self.assertEqual(nack.payload['event_type'], 'nack')
        self.assertEqual(nack.payload['user_message_id'], reply['message_id'])
        self.assertEqual(nack.payload['nack_reason'], reason)

    @inlineCallbacks
    def test_inbound_begin(self):
        user_content = "Who are you?"
        d = self.tx_helper.mk_request(ussdRequestString=user_content)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_NEW,
            content=user_content)

        reply_content = "We are the Knights Who Say ... Ni!"
        reply = msg.reply(reply_content)
        self.tx_helper.dispatch_outbound(reply)
        response = yield d
        self.assertEqual(json.loads(response.delivered_body), {
            "responseString": reply_content,
            "action": "request",
        })
        self.assertEqual(response.code, 200)

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assert_ack(ack, reply)

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        yield self.mk_session(self._transaction_id)

        user_content = "Well, what is it you want?"
        d = self.tx_helper.mk_request(ussdRequestString=user_content)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_RESUME,
            content=user_content)

        reply_content = "We want ... a shrubbery!"
        reply = msg.reply(reply_content, continue_session=False)
        self.tx_helper.dispatch_outbound(reply)
        response = yield d
        self.assertEqual(json.loads(response.delivered_body), {
            "responseString": reply_content,
            "action": "end",
        })
        self.assertEqual(response.code, 200)

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assert_ack(ack, reply)

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_resume(self):
        yield self.mk_session()

        user_content = "Well, what is it you want?"
        d = self.tx_helper.mk_request(ussdRequestString=user_content)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(msg,
            session_event=TransportUserMessage.SESSION_RESUME,
            content=user_content)

        reply_content = "We want ... a shrubbery!"
        reply = msg.reply(reply_content, continue_session=True)
        self.tx_helper.dispatch_outbound(reply)
        response = yield d
        self.assertEqual(json.loads(response.delivered_body), {
            "responseString": reply_content,
            "action": "request",
        })
        self.assertEqual(response.code, 200)

        [ack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assert_ack(ack, reply)

    @inlineCallbacks
    def test_request_with_missing_parameters(self):
        response = yield self.tx_helper.mk_request_raw(
            params={"ussdServiceCode": '', "msisdn": '', "creationTime": ''})

        json_resp = json.loads(response.delivered_body)
        json_resp['missing_parameter'] = sorted(json_resp['missing_parameter'])
        self.assertEqual(json_resp, {
            'missing_parameter': sorted([
                "transactionTime", "transactionId", "response",
                "ussdRequestString",
            ]),
        })
        self.assertEqual(response.code, 400)

    @inlineCallbacks
    def test_request_with_unexpected_parameters(self):
        response = yield self.tx_helper.mk_request(
            unexpected_p1='', unexpected_p2='')

        self.assertEqual(response.code, 400)
        body = json.loads(response.delivered_body)
        self.assertEqual(set(['unexpected_parameter']), set(body.keys()))
        self.assertEqual(
            sorted(body['unexpected_parameter']),
            ['unexpected_p1', 'unexpected_p2'])

    @inlineCallbacks
    def test_nack_insufficient_message_fields(self):
        reply = self.tx_helper.make_outbound(
            None, message_id='23', in_reply_to=None)
        self.tx_helper.dispatch_outbound(reply)
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assert_nack(nack, reply, 'Missing fields: in_reply_to, content')

    @inlineCallbacks
    def test_nack_http_http_response_failure(self):
        reply = self.tx_helper.make_outbound(
            'There are some who call me ... Tim!', message_id='23',
            in_reply_to='some-number')
        self.tx_helper.dispatch_outbound(reply)
        [nack] = yield self.tx_helper.wait_for_dispatched_events(1)
        self.assert_nack(
            nack, reply, 'Could not find original request.')
