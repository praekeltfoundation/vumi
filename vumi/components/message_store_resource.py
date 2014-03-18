# -*- test-case-name: vumi.components.tests.test_message_store_resource -*-
from functools import partial

from twisted.application.internet import StreamServerEndpointService
from twisted.internet.defer import DeferredList, inlineCallbacks
from twisted.web.resource import NoResource, Resource
from twisted.web.server import NOT_DONE_YET

from vumi.components.message_store import MessageStore
from vumi.components.message_store_exporter import MessageStoreExporter
from vumi.config import ConfigDict, ConfigText, ConfigServerEndpoint
from vumi.persist.txriak_manager import TxRiakManager
from vumi.persist.txredis_manager import TxRedisManager
from vumi.transports.httprpc import httprpc
from vumi.utils import build_web_site
from vumi.worker import BaseWorker


class MessageStoreProxyResource(Resource):

    isLeaf = True
    default_chunk_size = 10
    default_concurrency = 10

    def __init__(self, message_store, batch_id):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id

    def render_GET(self, request):
        resp_headers = request.responseHeaders
        resp_headers.addRawHeader(
            'Content-Type', 'application/json; charset=utf-8')
        if 'chunk_size' in request.args:
            chunk_size = int(request.args['chunk_size'][0])
        else:
            chunk_size = self.default_chunk_size

        if 'concurrency' in request.args:
            concurrency = int(request.args['concurrency'][0])
        else:
            concurrency = self.default_concurrency

        d = self.get_keys(self.message_store, self.batch_id)
        d.addCallback(
            self.export, self.message_store, chunk_size, concurrency, request)
        d.addCallback(lambda _: request.finish())
        return NOT_DONE_YET

    def export(self, keys, message_store, chunk_size, concurrency, request):
        raise NotImplemented('To be implemented by sub-class.')

    def get_keys(self, message_store, batch_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def write_message(self, message, request):
        request.write(message.to_json())
        request.write('\n')


class InboundResource(MessageStoreProxyResource):

    def get_keys(self, message_store, batch_id):
        return message_store.batch_inbound_keys(batch_id)

    def export(self, keys, message_store, chunk_size, concurrency, request):
        exporter = MessageStoreExporter(chunk_size, concurrency)
        return exporter.export(
            keys, partial(self.handle_key, message_store, request))

    def handle_key(self, message_store, request, message_id):
        d = message_store.get_inbound_message(message_id)
        d.addCallback(self.write_message, request)
        return d


class OutboundResource(MessageStoreProxyResource):

    def get_keys(self, message_store, batch_id):
        return message_store.batch_outbound_keys(batch_id)

    def export(self, keys, message_store, chunk_size, concurrency, request):
        exporter = MessageStoreExporter(chunk_size, concurrency)
        return exporter.export(
            keys, partial(self.handle_key, message_store, request))

    def handle_key(self, message_store, request, message_id):
        d = message_store.get_outbound_message(message_id)
        d.addCallback(self.write_message, request)
        return d


class BatchResource(Resource):

    def __init__(self, message_store, batch_id):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id

    def getChild(self, path, request):
        resource_class = {
            'inbound.json': InboundResource,
            'outbound.json': OutboundResource,
        }.get(path)
        if resource_class is None:
            return NoResource()
        return resource_class(self.message_store, self.batch_id)


class MessageStoreResource(Resource):

    def __init__(self, message_store):
        Resource.__init__(self)
        self.message_store = message_store

    def getChild(self, path, request):
        return BatchResource(self.message_store, path)


class MessageStoreResourceWorker(BaseWorker):

    class CONFIG_CLASS(BaseWorker.CONFIG_CLASS):
        worker_name = ConfigText(
            'Name of the this message store resource worker',
            required=True, static=True)
        twisted_endpoint = ConfigServerEndpoint(
            'Twisted endpoint to listen on.', required=True, static=True)
        web_path = ConfigText(
            'The path to serve this resource on.', required=True, static=True)
        health_path = ConfigText(
            'The path to serve the health resource on.', default='/health/',
            static=True)
        riak_manager = ConfigDict(
            'Riak client configuration.', default={}, static=True)
        redis_manager = ConfigDict(
            'Redis client configuration.', default={}, static=True)

    def get_health_response(self):
        return 'OK'

    @inlineCallbacks
    def setup_worker(self):
        config = self.get_static_config()
        riak = yield TxRiakManager.from_config(config.riak_manager)
        redis = yield TxRedisManager.from_config(config.redis_manager)
        self.store = MessageStore(riak, redis)

        site = build_web_site({
            config.web_path: MessageStoreResource(self.store),
            config.health_path: httprpc.HttpRpcHealthResource(self),
        })
        self.addService(
            StreamServerEndpointService(config.twisted_endpoint, site))

    def teardown_worker(self):
        pass

    def setup_connectors(self):
        # NOTE: not doing anything AMQP
        pass
