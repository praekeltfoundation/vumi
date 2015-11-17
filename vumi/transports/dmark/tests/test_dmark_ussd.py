# -*- coding: utf-8 -*-

"""Tests for vumi.transports.dmark.dmark_ussd."""

import json

import urllib

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import Clock

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
        self.clock = Clock()
        self.patch(DmarkUssdTransport, 'get_clock', lambda _: self.clock)

        self.config = {
            'web_port': 0,
            'web_path': '/api/v1/dmark/ussd/',
            'publish_status': True,
        }
        self.tx_helper = self.add_helper(
            HttpRpcTransportHelper(DmarkUssdTransport,
                                   request_defaults=self._request_defaults))
        self.transport = yield self.tx_helper.get_transport(self.config)
        self.session_manager = self.transport.session_manager
        self.transport_url = self.transport.get_transport_url(
            self.config['web_path'])
        yield self.session_manager.redis._purge_all()  # just in case
        self.session_timestamps = {}

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
        self.assert_inbound_message(
            msg,
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
    def test_inbound_status(self):
        d = self.tx_helper.mk_request()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.tx_helper.dispatch_outbound(msg.reply('foo'))
        yield d

        self.assertEqual(status['status'], 'ok')
        self.assertEqual(status['component'], 'request')
        self.assertEqual(status['type'], 'request_parsed')
        self.assertEqual(status['message'], 'Request parsed')

    @inlineCallbacks
    def test_inbound_cannot_decode(self):
        '''If the content cannot be decoded, an error shoould be sent back'''
        user_content = "Who are you?".encode('utf-32')
        response = yield self.tx_helper.mk_request(
            ussdRequestString=user_content)
        self.assertEqual(response.code, 400)

        body = json.loads(response.delivered_body)
        request = body['invalid_request']
        self.assertEqual(request['content'], '')
        self.assertEqual(request['path'], self.config['web_path'])
        self.assertEqual(request['method'], 'GET')
        self.assertEqual(request['headers']['Connection'], ['close'])
        encoded_str = urllib.urlencode({'ussdRequestString': user_content})
        self.assertTrue(encoded_str in request['uri'])

    @inlineCallbacks
    def test_inbound_cannot_decode_status(self):
        '''If the request cannot be decoded, a status event should be sent'''
        user_content = "Who are you?".encode('utf-32')
        yield self.tx_helper.mk_request(ussdRequestString=user_content)

        [status] = self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['component'], 'request')
        self.assertEqual(status['status'], 'down')
        self.assertEqual(status['type'], 'invalid_encoding')
        self.assertEqual(status['message'], 'Invalid encoding')

        request = status['details']['request']
        self.assertEqual(request['content'], '')
        self.assertEqual(request['path'], self.config['web_path'])
        self.assertEqual(request['method'], 'GET')
        self.assertEqual(request['headers']['Connection'], ['close'])
        encoded_str = urllib.urlencode({'ussdRequestString': user_content})
        self.assertTrue(encoded_str in request['uri'])

    @inlineCallbacks
    def test_inbound_resume_and_reply_with_end(self):
        yield self.mk_session(self._transaction_id)

        user_content = "Well, what is it you want?"
        d = self.tx_helper.mk_request(ussdRequestString=user_content)
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        self.assert_inbound_message(
            msg,
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
        self.assert_inbound_message(
            msg,
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
    def test_status_with_missing_parameters(self):
        '''A request with missing parameters should send a TransportStatus
        with the relevant details.'''
        yield self.tx_helper.mk_request_raw(
            params={"ussdServiceCode": '', "msisdn": '', "creationTime": ''})

        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['status'], 'down')
        self.assertEqual(status['component'], 'request')
        self.assertEqual(status['type'], 'invalid_inbound_fields')
        self.assertEqual(sorted(status['details']['missing_parameter']), [
            'response', 'transactionId', 'transactionTime',
            'ussdRequestString'])

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
    def test_status_with_unexpected_parameters(self):
        '''A request with unexpected parameters should send a TransportStatus
        with the relevant details.'''
        yield self.tx_helper.mk_request(
            unexpected_p1='', unexpected_p2='')

        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['status'], 'down')
        self.assertEqual(status['component'], 'request')
        self.assertEqual(status['type'], 'invalid_inbound_fields')
        self.assertEqual(sorted(status['details']['unexpected_parameter']), [
            'unexpected_p1', 'unexpected_p2'])

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

    @inlineCallbacks
    def test_status_quick_response(self):
        '''Ok status event should be sent if the response is quick.'''
        d = self.tx_helper.mk_request()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.tx_helper.dispatch_outbound(msg.reply('foo'))
        yield d

        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['status'], 'ok')
        self.assertEqual(status['component'], 'response')
        self.assertEqual(status['message'], 'Response sent')
        self.assertEqual(status['type'], 'response_sent')

    @inlineCallbacks
    def test_status_degraded_slow_response(self):
        '''A degraded status event should be sent if the response took longer
        than 1 second.'''
        d = self.tx_helper.mk_request()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.clock.advance(self.transport.response_time_degraded + 0.1)

        self.tx_helper.dispatch_outbound(msg.reply('foo'))
        yield d

        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['status'], 'degraded')
        self.assertTrue(
            str(self.transport.response_time_degraded) in status['reasons'][0])
        self.assertEqual(status['component'], 'response')
        self.assertEqual(status['type'], 'slow_response')
        self.assertEqual(status['message'], 'Slow response')

    @inlineCallbacks
    def test_status_down_very_slow_response(self):
        '''A down status event should be sent if the response took longer
        than 10 seconds.'''
        d = self.tx_helper.mk_request()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.clock.advance(self.transport.response_time_down + 0.1)

        self.tx_helper.dispatch_outbound(msg.reply('foo'))
        yield d

        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['status'], 'down')
        self.assertTrue(
            str(self.transport.response_time_down) in status['reasons'][0])
        self.assertEqual(status['component'], 'response')
        self.assertEqual(status['type'], 'very_slow_response')
        self.assertEqual(status['message'], 'Very slow response')

    @inlineCallbacks
    def test_no_response_status_for_message_not_found(self):
        '''If we cannot find the starting timestamp for a message, no status
        message should be sent'''
        reply = self.tx_helper.make_outbound(
            'There are some who call me ... Tim!', message_id='23',
            in_reply_to='some-number')
        self.tx_helper.dispatch_outbound(reply)
        statuses = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(len(statuses), 0)

    @inlineCallbacks
    def test_no_good_status_event_for_bad_responses(self):
        '''If the http response is not a good (200-399) response, then a
        status event shouldn't be sent, because we send different status
        events for those errors.'''
        d = self.tx_helper.mk_request()

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.transport.finish_request(msg['message_id'], '', code=500)

        yield d

        statuses = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(len(statuses), 0)

    @inlineCallbacks
    def test_no_degraded_status_event_for_bad_responses(self):
        '''If the http response is not a good (200-399) response, then a
        status event shouldn't be sent, because we send different status
        events for those errors.'''
        d = self.tx_helper.mk_request()

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.clock.advance(self.transport.response_time_degraded + 0.1)

        self.transport.finish_request(msg['message_id'], '', code=500)

        yield d

        statuses = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(len(statuses), 0)

    @inlineCallbacks
    def test_no_down_status_event_for_bad_responses(self):
        '''If the http response is not a good (200-399) response, then a
        status event shouldn't be sent, because we send different status
        events for those errors.'''
        d = self.tx_helper.mk_request()

        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.clock.advance(self.transport.response_time_down + 0.1)

        self.transport.finish_request(msg['message_id'], '', code=500)

        yield d

        statuses = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(len(statuses), 0)

    @inlineCallbacks
    def test_status_down_timeout(self):
        '''A down status event should be sent if the response timed out'''
        d = self.tx_helper.mk_request()
        [msg] = yield self.tx_helper.wait_for_dispatched_inbound(1)
        yield self.tx_helper.clear_dispatched_statuses()

        self.clock.advance(self.transport.request_timeout + 0.1)

        self.tx_helper.dispatch_outbound(msg.reply('foo'))
        yield d

        [status] = yield self.tx_helper.get_dispatched_statuses()
        self.assertEqual(status['status'], 'down')
        self.assertTrue(
            str(self.transport.request_timeout) in status['reasons'][0])
        self.assertEqual(status['component'], 'response')
        self.assertEqual(status['type'], 'timeout')
        self.assertEqual(status['message'], 'Response timed out')
        self.assertEqual(status['details'], {
            'response_time': self.transport.request_timeout + 0.1,
        })
