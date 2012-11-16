import json

from twisted.trial.unittest import TestCase
from twisted.web import server
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from vumi.components.message_store_api import MessageStoreAPI
from vumi.components.message_store import MessageStore
from vumi.utils import http_request_full
from vumi.tests.utils import PersistenceMixin, MessageMakerMixin


class MessageStoreAPITestCase(TestCase, MessageMakerMixin, PersistenceMixin):

    use_riak = True

    @inlineCallbacks
    def setUp(self):
        self._persist_setUp()

        self.riak = yield self.get_riak_manager()
        self.redis = yield self.get_redis_manager()
        self.store = MessageStore(self.riak, self.redis)
        self.tag = ("pool", "tag")
        self.batch_id = yield self.store.batch_start([self.tag])

        factory = server.Site(MessageStoreAPI(self.store))
        self.webserver = yield reactor.listenTCP(0, factory)
        self.addr = self.webserver.getHost()
        self.url = 'http://%s:%s/' % (self.addr.host, self.addr.port)

    @inlineCallbacks
    def create_inbound(self, batch_id, count, content_template):
        for i in range(count):
            msg = self.mkmsg_in(content=content_template.format(i))
            yield self.add_inbound_message(msg, batch_id=batch_id)

    @inlineCallbacks
    def create_outbound(self, batch_id, count, content_template):
        for i in range(count):
            msg = self.mkmsg_out(content=content_template.format(i))
            yield self.add_outbound_message(msg, batch_id=batch_id)

    @inlineCallbacks
    def tearDown(self):
        yield self._persist_tearDown()
        self.webserver.loseConnection()

    def do_get(self, path):
        url = '%s%s' % (self.url, path)
        return http_request_full(url, method='GET')

    def do_post(self, path, data):
        url = '%s%s' % (self.url, path)
        return http_request_full(url, data=json.dumps(data), headers={
                'Content-Type': 'application/json; charset=utf-8',
            }, method='POST')

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
    def test_batch_search_resource(self):
        response = yield self.do_post('batch/%s/search/' % (self.batch_id,), [
            {'key': 'msg.content', 'pattern': '.*', 'flags': ''}])
        self.assertEqual(response.delivered_body, self.batch_id)
        self.assertEqual(response.code, 200)
