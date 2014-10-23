# -*- test-case-name: vumi.components.tests.test_message_store_resource -*-

from csv import writer

from zope.interface import Interface, implements

from twisted.application.internet import StreamServerEndpointService
from twisted.internet.defer import DeferredList, inlineCallbacks
from twisted.web.resource import NoResource, Resource
from twisted.web.server import NOT_DONE_YET

from vumi.components.message_store import MessageStore
from vumi.config import (
    ConfigDict, ConfigText, ConfigServerEndpoint, ConfigInt,
    ServerEndpointFallback)
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


class IMessageFormatter(Interface):
    """ Interface for writing messages to an HTTP request. """

    def add_http_headers(request):
        """
        Add any needed HTTP headers to the request.

        Often used to set the Content-Type header.
        """

    def write_row_header(request):
        """
        Write any header bytes that need to be written to the request before
        messages.
        """

    def write_row(request, message):
        """
        Write a :class:`TransportUserMessage` to the request.
        """


class JsonFormatter(object):
    """ Formatter for writing messages to requests as JSON. """

    implements(IMessageFormatter)

    def add_http_headers(self, request):
        resp_headers = request.responseHeaders
        resp_headers.addRawHeader(
            'Content-Type', 'application/json; charset=utf-8')

    def write_row_header(self, request):
        pass

    def write_row(self, request, message):
        request.write(message.to_json())
        request.write('\n')


class CsvFormatter(object):
    """ Formatter for writing messages to requests as CSV. """

    implements(IMessageFormatter)

    FIELDS = (
        'message_id',
        'to_addr',
        'from_addr',
        'in_reply_to',
        'session_event',
        'content',
        'group',
    )

    def add_http_headers(self, request):
        resp_headers = request.responseHeaders
        resp_headers.addRawHeader(
            'Content-Type', 'text/csv; charset=utf-8')

    def write_row_header(self, request):
        writer(request).writerow(self.FIELDS)

    def write_row(self, request, message):
        writer(request).writerow(list(
            (message[key] or '').encode('utf-8')
            for key in self.FIELDS))


class MessageStoreProxyResource(Resource):

    isLeaf = True
    default_concurrency = 10

    def __init__(self, message_store, batch_id, formatter):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id
        self.formatter = formatter

    def render_GET(self, request):
        self.formatter.add_http_headers(request)
        self.formatter.write_row_header(request)

        if 'concurrency' in request.args:
            concurrency = int(request.args['concurrency'][0])
        else:
            concurrency = self.default_concurrency

        d = self.get_keys_page(self.message_store, self.batch_id)
        d.addCallback(self.fetch_pages, concurrency, request)
        return NOT_DONE_YET

    def get_keys_page(self, message_store, batch_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def get_message(self, message_store, message_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def fetch_pages(self, keys_page, concurrency, request):
        """
        Process a page of keys and each subsequent page.

        The keys for the current page are handed off to :meth:`fetch_page` for
        processing. If there is another page, we fetch that while the current
        page is being handled and add a callback to process it when the
        current page is finished.

        When there are no more pages, we add a callback to close the request.
        """
        d = self.fetch_page(keys_page, concurrency, request)
        if keys_page.has_next_page():
            # We fetch the next page before waiting for the current page to be
            # processed.
            next_page_d = keys_page.next_page()
            d.addCallback(lambda _: next_page_d)
            # Add this method as a callback to operate on the next page. It's
            # like recursion, but without worrying about stack size.
            d.addCallback(self.fetch_pages, concurrency, request)
        else:
            # No more pages, so close the request.
            d.addCallback(lambda _: request.finish())
        return d

    @inlineCallbacks
    def fetch_page(self, keys_page, concurrency, request):
        """
        Process a page of keys in chunks of concurrently-fetched messages.
        """
        for keys in chunks(list(keys_page), concurrency):
            yield self.handle_chunk(keys, request)

    def handle_chunk(self, message_keys, request):
        """
        Concurrently fetch a chunk of messages and write each to the response.
        """
        return DeferredList([
            self.handle_message(key, request) for key in message_keys])

    def handle_message(self, message_key, request):
        d = self.get_message(self.message_store, message_key)
        d.addCallback(self.write_message, request)
        return d

    def write_message(self, message, request):
        self.formatter.write_row(request, message)


class InboundResource(MessageStoreProxyResource):

    def get_keys_page(self, message_store, batch_id):
        return message_store.batch_inbound_keys_page(batch_id)

    def get_message(self, message_store, message_id):
        return message_store.get_inbound_message(message_id)


class OutboundResource(MessageStoreProxyResource):

    def get_keys_page(self, message_store, batch_id):
        return message_store.batch_outbound_keys_page(batch_id)

    def get_message(self, message_store, message_id):
        return message_store.get_outbound_message(message_id)


class BatchResource(Resource):

    RESOURCES = {
        'inbound.json': (InboundResource, JsonFormatter),
        'outbound.json': (OutboundResource, JsonFormatter),
        'inbound.csv': (InboundResource, CsvFormatter),
        'outbound.csv': (OutboundResource, CsvFormatter),
    }

    def __init__(self, message_store, batch_id):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id

    def getChild(self, path, request):
        if path not in self.RESOURCES:
            return NoResource()
        resource_class, message_formatter = self.RESOURCES.get(path)
        return resource_class(
            self.message_store, self.batch_id, message_formatter())


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
            'Twisted endpoint to listen on.', required=True, static=True,
            fallbacks=[ServerEndpointFallback()])
        web_path = ConfigText(
            'The path to serve this resource on.', required=True, static=True)
        health_path = ConfigText(
            'The path to serve the health resource on.', default='/health/',
            static=True)
        riak_manager = ConfigDict(
            'Riak client configuration.', default={}, static=True)
        redis_manager = ConfigDict(
            'Redis client configuration.', default={}, static=True)

        # TODO: Deprecate these fields when confmodel#5 is done.
        host = ConfigText(
            "*DEPRECATED* 'host' and 'port' fields may be used in place of"
            " the 'twisted_endpoint' field.", static=True)
        port = ConfigInt(
            "*DEPRECATED* 'host' and 'port' fields may be used in place of"
            " the 'twisted_endpoint' field.", static=True)

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
