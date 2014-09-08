import json
from datetime import datetime, timedelta

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from vumi.utils import http_request_full
from vumi.message import TransportUserMessage

from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, WorkerHelper, PersistenceHelper, import_skip,
)


class TestMessageStoreAPI(VumiTestCase):
    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(
            PersistenceHelper(use_riak=True))
        try:
            from vumi.components.message_store_api import (
                MatchResource, MessageStoreAPIWorker)
        except ImportError, e:
            import_skip(e, 'riak')

        self.msg_helper = self.add_helper(MessageHelper())
        self.worker_helper = self.add_helper(WorkerHelper())

        self.match_resource = MatchResource
        self.base_path = '/api/v1/'
        self.worker = yield self.worker_helper.get_worker(
            MessageStoreAPIWorker, self.persistence_helper.mk_config({
                'web_path': self.base_path,
                'web_port': 0,
                'health_path': '/health/',
            }))
        self.store = self.worker.store
        self.addr = self.worker.webserver.getHost()
        self.url = 'http://%s:%s%s' % (self.addr.host, self.addr.port,
                                        self.base_path)

        self.tag = ("pool", "tag")
        self.batch_id = yield self.store.batch_start([self.tag])

    @inlineCallbacks
    def create_inbound(self, batch_id, count, content_template):
        messages = []
        now = datetime.now()
        for i in range(count):
            msg = self.msg_helper.make_inbound(
                content_template.format(i),
                timestamp=(now - timedelta(i * 10)))
            yield self.store.add_inbound_message(msg, batch_id=batch_id)
            messages.append(msg)
        returnValue(messages)

    @inlineCallbacks
    def create_outbound(self, batch_id, count, content_template):
        messages = []
        now = datetime.now()
        for i in range(count):
            msg = self.msg_helper.make_outbound(
                content_template.format(i),
                timestamp=(now - timedelta(i * 10)))
            yield self.store.add_outbound_message(msg, batch_id=batch_id)
            messages.append(msg)
        returnValue(messages)

    def do_get(self, path, headers={}):
        url = '%s%s' % (self.url, path)
        return http_request_full(url, headers=headers, method='GET')

    def do_post(self, path, data, headers={}):
        url = '%s%s' % (self.url, path)
        default_headers = {
                'Content-Type': 'application/json; charset=utf-8',
            }
        default_headers.update(headers)
        return http_request_full(url, data=json.dumps(data),
            headers=default_headers, method='POST')

    def wait_for_results(self, direction, batch_id, token):
        url = '%sbatch/%s/%s/match/?token=%s' % (self.url, batch_id,
                                                        direction, token)

        @inlineCallbacks
        def check(d):
            response = yield http_request_full(url, method='GET')
            [progress_status] = response.headers.getRawHeaders(
                self.match_resource.RESP_IN_PROGRESS_HEADER)
            if progress_status == '0':
                d.callback(response)
            else:
                reactor.callLater(0, check, d)

        done = Deferred()
        reactor.callLater(0, check, done)
        return done

    @inlineCallbacks
    def do_query(self, direction, batch_id, pattern, key='msg.content',
                    flags='i', wait=False):
        query = [{
            'key': key,
            'pattern': pattern,
            'flags': flags,
        }]
        if wait:
            headers = {self.match_resource.REQ_WAIT_HEADER: '1'}
        else:
            headers = {}

        expected_token = self.store.cache.get_query_token(direction, query)
        response = yield self.do_post('batch/%s/%s/match/' % (
            self.batch_id, direction), query, headers=headers)
        [token] = response.headers.getRawHeaders(
            self.match_resource.RESP_TOKEN_HEADER)
        self.assertEqual(token, expected_token)
        self.assertEqual(response.code, 200)
        returnValue(token)

    def assertResultCount(self, response, count):
        self.assertEqual(
            response.headers.getRawHeaders(
                self.match_resource.RESP_COUNT_HEADER),
            [str(count)])

    def assertJSONResultEqual(self, json_blob, messages):
        """
        Asserts that the JSON response we're getting back is the same as
        the list of messages provided.

        There are easier ways to do this by comparing bigger JSON blogs
        but then debugging the huge strings would be a pain.
        """
        dictionaries = json.loads(json_blob)
        self.assertEqual(len(dictionaries), len(messages),
            'Unequal amount of dictionaries and messages')
        for dictionary, message in zip(dictionaries, messages):
            # The json dumping & reloading happening here is required to have
            # the timestamp fields be parsed properly. This is an unfortunate
            # side effect of how how timestamps are currently stored as
            # datetime() instances in the payload instead of plain strings.
            self.assertEqual(
                TransportUserMessage(_process_fields=False, **message.payload),
                TransportUserMessage.from_json(json.dumps(dictionary)))

    @inlineCallbacks
    def test_batch_index_resource(self):
        response = yield self.do_get('batch/')
        self.assertEqual(response.delivered_body, '')
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_batch_resource(self):
        response = yield self.do_get('batch/%s/' % (self.batch_id))
        self.assertEqual(response.delivered_body, self.batch_id)
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_waiting_inbound_match_resource(self):
        messages = yield self.create_inbound(self.batch_id, 22,
                                                'hello world {0}')
        token = yield self.do_query('inbound', self.batch_id, '.*',
                                                wait=True)
        response = yield self.do_get('batch/%s/inbound/match/?token=%s' % (
            self.batch_id, token))
        self.assertResultCount(response, 22)
        current_page = messages[:self.match_resource.DEFAULT_RESULT_SIZE]
        self.assertJSONResultEqual(response.delivered_body, current_page)
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_keys_inbound_match_resource(self):
        messages = yield self.create_inbound(self.batch_id, 22,
                                                'hello world {0}')
        token = yield self.do_query('inbound', self.batch_id, '.*',
                                                wait=True)
        response = yield self.do_get(
            'batch/%s/inbound/match/?token=%s&keys=1' % (
                self.batch_id, token))
        self.assertResultCount(response, 22)
        current_page = messages[:self.match_resource.DEFAULT_RESULT_SIZE]
        self.assertEqual(json.loads(response.delivered_body),
            [msg['message_id'] for msg in current_page])
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_polling_inbound_match_resource(self):
        messages = yield self.create_inbound(self.batch_id, 22,
                                                'hello world {0}')
        token = yield self.do_query('inbound', self.batch_id, '.*',
                                                wait=False)
        response = yield self.wait_for_results('inbound', self.batch_id, token)
        self.assertResultCount(response, 22)
        page = messages[:20]
        self.assertJSONResultEqual(response.delivered_body, page)
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_empty_inbound_match_resource(self):
        expected_token = yield self.do_query('inbound', self.batch_id, '.*')
        response = yield self.do_get('batch/%s/inbound/match/?token=%s' % (
            self.batch_id, expected_token))
        self.assertResultCount(response, 0)
        self.assertEqual(json.loads(response.delivered_body), [])
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_waiting_outbound_match_resource(self):
        messages = yield self.create_outbound(self.batch_id, 22,
                                                'hello world {0}')
        token = yield self.do_query('outbound', self.batch_id, '.*',
                                                wait=True)
        response = yield self.do_get('batch/%s/outbound/match/?token=%s' % (
            self.batch_id, token))
        self.assertResultCount(response, 22)
        current_page = messages[:self.match_resource.DEFAULT_RESULT_SIZE]
        self.assertJSONResultEqual(response.delivered_body, current_page)
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_keys_outbound_match_resource(self):
        messages = yield self.create_outbound(self.batch_id, 22,
                                                'hello world {0}')
        token = yield self.do_query('outbound', self.batch_id, '.*',
                                                wait=True)
        response = yield self.do_get(
            'batch/%s/outbound/match/?token=%s&keys=1' % (
                self.batch_id, token))
        self.assertResultCount(response, 22)
        current_page = messages[:self.match_resource.DEFAULT_RESULT_SIZE]
        self.assertEqual(json.loads(response.delivered_body),
            [msg['message_id'] for msg in current_page])
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_polling_outbound_match_resource(self):
        messages = yield self.create_outbound(self.batch_id, 22,
                                                'hello world {0}')
        token = yield self.do_query('outbound', self.batch_id, '.*',
                                                wait=False)
        response = yield self.wait_for_results('outbound', self.batch_id,
                                                token)
        self.assertResultCount(response, 22)
        page = messages[:20]
        self.assertJSONResultEqual(response.delivered_body, page)
        self.assertEqual(response.code, 200)

    @inlineCallbacks
    def test_empty_outbound_match_resource(self):
        expected_token = yield self.do_query('outbound', self.batch_id, '.*')
        response = yield self.do_get('batch/%s/outbound/match/?token=%s' % (
            self.batch_id, expected_token))
        self.assertResultCount(response, 0)
        self.assertEqual(json.loads(response.delivered_body), [])
        self.assertEqual(response.code, 200)
