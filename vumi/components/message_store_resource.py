# -*- test-case-name: vumi.components.tests.test_message_store_resource -*-

from twisted.application.internet import StreamServerEndpointService
from twisted.internet.defer import DeferredList, inlineCallbacks
from twisted.web.resource import NoResource, Resource
from twisted.web.server import NOT_DONE_YET

from vumi.components.message_store import MessageStore
from vumi.config import ConfigDict, ConfigText, ConfigServerEndpoint
from vumi.persist.txriak_manager import TxRiakManager
from vumi.persist.txredis_manager import TxRedisManager
from vumi.transports.httprpc import httprpc
from vumi.utils import build_web_site
from vumi.worker import BaseWorker


# NOTE: Thanks Ned http://stackoverflow.com/a/312464!
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


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
        d.addCallback(lambda keys: list(chunks(keys, chunk_size)))
        d.addCallback(self.fetch_chunks, concurrency, request)
        return NOT_DONE_YET

    def get_keys(self, message_store, batch_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def get_message(self, message_store, message_id):
        raise NotImplementedError('To be implemented by sub-class.')

    @inlineCallbacks
    def fetch_chunks(self, chunked_keys, concurrency, request):
        while chunked_keys:
            block, chunked_keys = (
                chunked_keys[:concurrency], chunked_keys[concurrency:])
            yield self.handle_chunks(block, request)
        request.finish()

    def handle_chunks(self, chunks, request):
        return DeferredList([
            self.handle_chunk(chunk, request) for chunk in chunks])

    def handle_chunk(self, message_keys, request):
        return DeferredList([
            self.handle_message(key, request) for key in message_keys])

    def handle_message(self, message_key, request):
        d = self.get_message(self.message_store, message_key)
        d.addCallback(self.write_message, request)
        return d

    def write_message(self, message, request):
        request.write(message.to_json())
        request.write('\n')


class InboundResource(MessageStoreProxyResource):

    def get_keys(self, message_store, batch_id):
        return message_store.batch_inbound_keys(batch_id)

    def get_message(self, message_store, message_id):
        return message_store.get_inbound_message(message_id)


class OutboundResource(MessageStoreProxyResource):

    def get_keys(self, message_store, batch_id):
        return message_store.batch_outbound_keys(batch_id)

    def get_message(self, message_store, message_id):
        return message_store.get_outbound_message(message_id)


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
