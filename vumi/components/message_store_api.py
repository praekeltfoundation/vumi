# -*- test-case-name: vumi.components.tests.test_message_store_api -*-
import json
import functools

from twisted.web import resource
from twisted.web.server import NOT_DONE_YET
from twisted.internet.defer import inlineCallbacks


class MatchResource(resource.Resource):
    """
    A Resource that accepts a query as JSON via HTTP POST and issues a match
    operation on the MessageStore.
    """

    TTL_HEADER = 'X-VMS-Match-TTL'
    RESULT_COUNT_HEADER = 'X-VMS-Result-Count'

    def __init__(self, direction, message_store, batch_id):
        resource.Resource.__init__(self)
        self._match_cb = functools.partial(getattr(message_store,
            "find_%s_keys_matching" % (direction,)), batch_id)
        self._results_cb = functools.partial(
            message_store.get_keys_for_token, batch_id)
        self._count_cb = functools.partial(
            message_store.count_keys_for_token, batch_id)

    def _render_token(self, token, request):
        request.write(token)
        request.finish()

    def render_POST(self, request):
        ttl = int(request.headers.get(self.TTL_HEADER, 0))
        query = json.loads(request.content.read())
        deferred = self._match_cb(query, ttl=(ttl or None))
        deferred.addCallback(self._render_token, request)
        return NOT_DONE_YET

    def _add_count_header(self, count, token, request):
        return token

    @inlineCallbacks
    def _render_keys(self, request, token, start, stop, asc):
        count = yield self._count_cb(token)
        keys = yield self._results_cb(start, stop, asc)
        request.responseHeaders.addRawHeader(self.RESULT_COUNT_HEADER, count)
        request.write(json.dumps(keys))
        request.finish()

    def render_GET(self, request):
        token = request.args['token'][0]
        start = (request.args['start'][0] if 'start' in request.args else 0)
        stop = (request.args['stop'][0] if 'stop' in request.args else 20)
        asc = (True if 'asc' in request.args else False)
        self._render_keys(request, token, int(start), int(stop), asc)
        return NOT_DONE_YET

    def getChild(self, name, request):
        return self


class BatchResource(resource.Resource):

    def __init__(self, message_store, batch_id):
        resource.Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id

        inbound = resource.Resource()
        inbound.putChild('match',
            MatchResource('inbound', message_store, batch_id))
        self.putChild('inbound', inbound)

        outbound = resource.Resource()
        outbound.putChild('match',
            MatchResource('outbound', message_store, batch_id))
        self.putChild('outbound', outbound)

    def render_GET(self, request):
        return self.batch_id

    def getChild(self, name, request):
        if not name:
            return self


class BatchIndexResource(resource.Resource):

    def __init__(self, message_store):
        resource.Resource.__init__(self)
        self.message_store = message_store

    def render_GET(self, request):
        return ''

    def getChild(self, batch_id, request):
        if batch_id:
            return BatchResource(self.message_store, batch_id)
        return self


class MessageStoreAPI(resource.Resource):

    def __init__(self, message_store):
        resource.Resource.__init__(self)
        self.putChild('batch', BatchIndexResource(message_store))
