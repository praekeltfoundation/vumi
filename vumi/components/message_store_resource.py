# -*- test-case-name: vumi.components.tests.test_message_store_resource -*-

import iso8601

from twisted.application.internet import StreamServerEndpointService
from twisted.internet.defer import DeferredList, inlineCallbacks
from twisted.web.resource import NoResource, Resource
from twisted.web.server import NOT_DONE_YET

from vumi.components.message_store import MessageStore
from vumi.components.message_formatters import JsonFormatter, CsvFormatter
from vumi.config import (
    ConfigDict, ConfigText, ConfigServerEndpoint, ConfigInt,
    ServerEndpointFallback)
from vumi.message import format_vumi_date
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


class ParameterError(Exception):
    """
    Exception raised while trying to parse a parameter.
    """
    pass


class MessageStoreProxyResource(Resource):

    isLeaf = True
    default_concurrency = 1

    def __init__(self, message_store, batch_id, formatter):
        Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id
        self.formatter = formatter

    def _extract_date_arg(self, request, argname):
        if argname not in request.args:
            return None
        if len(request.args[argname]) > 1:
            raise ParameterError(
                "Invalid '%s' parameter: Too many values" % (argname,))
        [value] = request.args[argname]
        try:
            timestamp = iso8601.parse_date(value)
            return format_vumi_date(timestamp)
        except iso8601.ParseError as e:
            raise ParameterError(
                "Invalid '%s' parameter: %s" % (argname, str(e)))

    def render_GET(self, request):
        if 'concurrency' in request.args:
            concurrency = int(request.args['concurrency'][0])
        else:
            concurrency = self.default_concurrency

        try:
            start = self._extract_date_arg(request, 'start')
            end = self._extract_date_arg(request, 'end')
        except ParameterError as e:
            request.setResponseCode(400)
            return str(e)

        self.formatter.add_http_headers(request)
        self.formatter.write_row_header(request)

        if not (start or end):
            d = self.get_keys_page(self.message_store, self.batch_id)
        else:
            d = self.get_keys_page_for_time(
                self.message_store, self.batch_id, start, end)
        request.connection_has_been_closed = False
        request.notifyFinish().addBoth(
            lambda _: setattr(request, 'connection_has_been_closed', True))
        d.addCallback(self.fetch_pages, concurrency, request)
        return NOT_DONE_YET

    def get_keys_page(self, message_store, batch_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def get_keys_page_for_time(self, message_store, batch_id, start, end):
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
        if request.connection_has_been_closed:
            # We're no longer connected, so stop doing work.
            return
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
            d.addCallback(self.finish_request_cb, request)
        return d

    def finish_request_cb(self, _result, request):
        if not request.connection_has_been_closed:
            # We need to check for this here in case we lose the connection
            # while delivering the last page.
            return request.finish()

    @inlineCallbacks
    def fetch_page(self, keys_page, concurrency, request):
        """
        Process a page of keys in chunks of concurrently-fetched messages.
        """
        for keys in chunks(list(keys_page), concurrency):
            if request.connection_has_been_closed:
                # We're no longer connected, so stop doing work.
                return
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

    def get_keys_page_for_time(self, message_store, batch_id, start, end):
        return message_store.batch_inbound_keys_with_timestamps(
            batch_id, max_results=message_store.DEFAULT_MAX_RESULTS,
            start=start, end=end, with_timestamps=False)

    def get_message(self, message_store, message_id):
        return message_store.get_inbound_message(message_id)


class OutboundResource(MessageStoreProxyResource):

    def get_keys_page(self, message_store, batch_id):
        return message_store.batch_outbound_keys_page(batch_id)

    def get_keys_page_for_time(self, message_store, batch_id, start, end):
        return message_store.batch_outbound_keys_with_timestamps(
            batch_id, max_results=message_store.DEFAULT_MAX_RESULTS,
            start=start, end=end, with_timestamps=False)

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
