# -*- coding: utf-8 -*-

import json
from datetime import datetime
from urllib import urlencode

from twisted.internet import reactor
from twisted.internet.defer import (
    inlineCallbacks, Deferred, succeed, gatherResults)
from twisted.web.server import Site

from vumi.components.message_formatters import JsonFormatter

from vumi.utils import http_request_full

from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, PersistenceHelper, import_skip,
    WorkerHelper)


class TestMessageStoreResource(VumiTestCase):

    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        self.worker_helper = self.add_helper(WorkerHelper())
        self.msg_helper = self.add_helper(MessageHelper())

    @inlineCallbacks
    def start_server(self):
        try:
            from vumi.components.message_store_resource import (
                MessageStoreResourceWorker)
        except ImportError, e:
            import_skip(e, 'riak')

        config = self.persistence_helper.mk_config({
            'twisted_endpoint': 'tcp:0',
            'web_path': '/resource_path/',
        })

        worker = yield self.worker_helper.get_worker(
            MessageStoreResourceWorker, config)
        yield worker.startService()
        port = yield worker.services[0]._waitingForPort
        addr = port.getHost()

        self.url = 'http://%s:%s' % (addr.host, addr.port)
        self.store = worker.store
        self.addCleanup(self.stop_server, port)

    def stop_server(self, port):
        d = port.stopListening()
        d.addCallback(lambda _: port.loseConnection())
        return d

    def make_batch(self, tag):
        return self.store.batch_start([tag])

    def make_outbound(self, batch_id, content, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg = self.msg_helper.make_outbound(content, timestamp=timestamp)
        d = self.store.add_outbound_message(msg, batch_id=batch_id)
        d.addCallback(lambda _: msg)
        return d

    def make_inbound(self, batch_id, content, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        msg = self.msg_helper.make_inbound(content, timestamp=timestamp)
        d = self.store.add_inbound_message(msg, batch_id=batch_id)
        d.addCallback(lambda _: msg)
        return d

    def make_ack(self, batch_id, timestamp=None):
        if timestamp is None:
            timestamp = datetime.utcnow()
        ack = self.msg_helper.make_ack(timestamp=timestamp)
        d = self.store.add_event(ack, batch_ids=[batch_id])
        d.addCallback(lambda _: ack)
        return d

    def make_request(self, method, batch_id, leaf, **params):
        url = '%s/%s/%s/%s' % (self.url, 'resource_path', batch_id, leaf)
        if params:
            url = '%s?%s' % (url, urlencode(params))
        return http_request_full(method=method, url=url)

    def get_batch_resource(self, batch_id):
        return self.store_resource.getChild(batch_id, None)

    def assert_csv_rows(self, rows, expected):
        self.assertEqual(sorted(rows), sorted([
            row_template % {
                'id': msg['message_id'],
                'ts': msg['timestamp'].isoformat(),
            } for row_template, msg in expected
        ]))

    def assert_csv_event_rows(self, rows, expected):
        self.assertEqual(sorted(rows), sorted([
            row_template % {
                'id': ev['event_id'],
                'ts': ev['timestamp'].isoformat(),
                'msg_id': ev['user_message_id'],
            } for row_template, ev in expected
        ]))

    @inlineCallbacks
    def test_get_inbound(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_inbound(batch_id, 'føø')
        msg2 = yield self.make_inbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'inbound.json')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id']]))

    @inlineCallbacks
    def test_get_inbound_csv(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_inbound(batch_id, 'føø')
        msg2 = yield self.make_inbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'inbound.csv')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg1),
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg2),
        ])

    @inlineCallbacks
    def test_get_outbound(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_outbound(batch_id, 'føø')
        msg2 = yield self.make_outbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'outbound.json')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id']]))

    @inlineCallbacks
    def test_get_outbound_csv(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_outbound(batch_id, 'føø')
        msg2 = yield self.make_outbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'outbound.csv')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg1),
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg2),
        ])

    @inlineCallbacks
    def test_get_events(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        ack1 = yield self.make_ack(batch_id)
        ack2 = yield self.make_ack(batch_id)
        resp = yield self.make_request('GET', batch_id, 'events.json')
        events = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([ev['event_id'] for ev in events]),
            set([ack1['event_id'], ack2['event_id']]))

    @inlineCallbacks
    def test_get_events_csv(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        ack1 = yield self.make_ack(batch_id)
        ack2 = yield self.make_ack(batch_id)
        resp = yield self.make_request('GET', batch_id, 'events.csv')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,event_id,status,user_message_id,"
            "nack_reason"))
        self.assert_csv_event_rows(rows, [
            ("%(ts)s,%(id)s,ack,%(msg_id)s,", ack1),
            ("%(ts)s,%(id)s,ack,%(msg_id)s,", ack2),
        ])

    @inlineCallbacks
    def test_get_inbound_multiple_pages(self):
        yield self.start_server()
        self.store.DEFAULT_MAX_RESULTS = 1
        batch_id = yield self.make_batch(('foo', 'bar'))
        msg1 = yield self.make_inbound(batch_id, 'føø')
        msg2 = yield self.make_inbound(batch_id, 'føø')
        resp = yield self.make_request('GET', batch_id, 'inbound.json')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id']]))

    @inlineCallbacks
    def test_disconnect_kills_server(self):
        """
        If the client connection is lost, we stop processing the request.

        This test is a bit hacky, because it has to muck about inside the
        resource in order to pause and resume at appropriate places.
        """
        yield self.start_server()

        from vumi.components.message_store_resource import InboundResource

        batch_id = yield self.make_batch(('foo', 'bar'))
        msgs = [(yield self.make_inbound(batch_id, 'føø'))
                for _ in range(6)]

        class PausingInboundResource(InboundResource):
            def __init__(self, *args, **kw):
                InboundResource.__init__(self, *args, **kw)
                self.pause_after = 3
                self.pause_d = Deferred()
                self.resume_d = Deferred()
                self.fetch = {}

            def _finish_fetching(self, msg):
                self.fetch[msg['message_id']].callback(msg['message_id'])
                return msg

            def get_message(self, message_store, message_id):
                self.fetch[message_id] = Deferred()
                d = succeed(None)
                if self.pause_after > 0:
                    self.pause_after -= 1
                else:
                    if not self.pause_d.called:
                        self.pause_d.callback(None)
                    d.addCallback(lambda _: self.resume_d)
                d.addCallback(lambda _: InboundResource.get_message(
                    self, message_store, message_id))
                d.addCallback(self._finish_fetching)
                return d

        res = PausingInboundResource(self.store, batch_id, JsonFormatter())
        site = Site(res)
        server = yield reactor.listenTCP(0, site, interface='127.0.0.1')
        self.add_cleanup(server.loseConnection)
        addr = server.getHost()
        url = 'http://%s:%s?concurrency=2' % (addr.host, addr.port)

        resp_d = http_request_full(method='GET', url=url)
        # Wait until we've processed some messages.
        yield res.pause_d
        # Kill the client connection.
        yield resp_d.cancel()
        # Continue processing messages.
        res.resume_d.callback(None)

        # This will fail because we've cancelled the request. We don't care
        # about the exception, so we swallow it and move on.
        yield resp_d.addErrback(lambda _: None)

        # Wait for all the in-progress loads to finish.
        fetched_msg_ids = yield gatherResults(res.fetch.values())

        sorted_message_ids = sorted(msg['message_id'] for msg in msgs)
        self.assertEqual(set(fetched_msg_ids), set(sorted_message_ids[:4]))

    @inlineCallbacks
    def test_get_inbound_for_time_range(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_inbound_for_time_range_bad_args(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))

        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', start='foo')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Unable to parse date string 'foo'")

        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', end='bar')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'end' parameter: Unable to parse date string 'bar'")

        url = '%s/%s/%s/%s?start=foo&start=bar' % (
            self.url, 'resource_path', batch_id, 'inbound.json')
        resp = yield http_request_full(method='GET', url=url)
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Too many values")

    @inlineCallbacks
    def test_get_inbound_for_time_range_no_start(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        msg1 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_inbound_for_time_range_no_end(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        msg4 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.json', start='2014-11-02 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id'], msg4['message_id']]))

    @inlineCallbacks
    def test_get_inbound_csv_for_time_range(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_inbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_inbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'inbound.csv', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg2),
            ("%(ts)s,%(id)s,9292,+41791234567,,,føø,", msg3),
        ])

    @inlineCallbacks
    def test_get_outbound_for_time_range(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_outbound_for_time_range_bad_args(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))

        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', start='foo')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Unable to parse date string 'foo'")

        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', end='bar')
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'end' parameter: Unable to parse date string 'bar'")

        url = '%s/%s/%s/%s?start=foo&start=bar' % (
            self.url, 'resource_path', batch_id, 'outbound.json')
        resp = yield http_request_full(method='GET', url=url)
        self.assertEqual(resp.code, 400)
        self.assertEqual(
            resp.delivered_body,
            "Invalid 'start' parameter: Too many values")

    @inlineCallbacks
    def test_get_outbound_for_time_range_no_start(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        msg1 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', end='2014-11-04 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg1['message_id'], msg2['message_id'], msg3['message_id']]))

    @inlineCallbacks
    def test_get_outbound_for_time_range_no_end(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        msg4 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.json', start='2014-11-02 00:00:00')
        messages = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([msg['message_id'] for msg in messages]),
            set([msg2['message_id'], msg3['message_id'], msg4['message_id']]))

    @inlineCallbacks
    def test_get_outbound_csv_for_time_range(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(1))
        msg2 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(2))
        msg3 = yield self.make_outbound(batch_id, 'føø', timestamp=mktime(3))
        yield self.make_outbound(batch_id, 'føø', timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'outbound.csv', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        rows = resp.delivered_body.split('\r\n')
        header, rows = rows[0], rows[1:-1]
        self.assertEqual(header, (
            "timestamp,message_id,to_addr,from_addr,in_reply_to,session_event,"
            "content,group"))
        self.assert_csv_rows(rows, [
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg2),
            ("%(ts)s,%(id)s,+41791234567,9292,,,føø,", msg3),
        ])

    @inlineCallbacks
    def test_get_events_for_time_range(self):
        yield self.start_server()
        batch_id = yield self.make_batch(('foo', 'bar'))
        mktime = lambda day: datetime(2014, 11, day, 12, 0, 0)
        yield self.make_ack(batch_id, timestamp=mktime(1))
        ack2 = yield self.make_ack(batch_id, timestamp=mktime(2))
        ack3 = yield self.make_ack(batch_id, timestamp=mktime(3))
        yield self.make_ack(batch_id, timestamp=mktime(4))
        resp = yield self.make_request(
            'GET', batch_id, 'events.json', start='2014-11-02 00:00:00',
            end='2014-11-04 00:00:00')
        events = map(
            json.loads, filter(None, resp.delivered_body.split('\n')))
        self.assertEqual(
            set([ev['event_id'] for ev in events]),
            set([ack2['event_id'], ack3['event_id']]))
