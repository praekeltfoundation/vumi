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

    DEFAULT_RESULT_SIZE = 20

    REQ_TTL_HEADER = 'X-VMS-Match-TTL'
    REQ_WAIT_HEADER = 'X-VMS-Match-Wait'

    RESP_COUNT_HEADER = 'X-VMS-Result-Count'
    RESP_TOKEN_HEADER = 'X-VMS-Result-Token'
    RESP_IN_PROGRESS_HEADER = 'X-VMS-Match-In-Progress'

    def __init__(self, direction, message_store, batch_id):
        """
        :param str direction:
            Either 'inbound' or 'oubound', this is used to figure out which
            function needs to be called on the MessageStore.
        :param MessageStore message_store:
            Instance of the MessageStore.
        :param str batch_id:
            The batch_id to use to query on.
        """
        resource.Resource.__init__(self)
        self._match_cb = functools.partial(getattr(message_store,
            "find_%s_keys_matching" % (direction,)), batch_id)
        self._results_cb = functools.partial(
            message_store.get_keys_for_token, batch_id)
        self._count_cb = functools.partial(
            message_store.count_keys_for_token, batch_id)
        self._in_progress_cb = functools.partial(
            message_store.is_query_in_progress, batch_id)

    def _render_token(self, token, request):
        request.responseHeaders.addRawHeader(self.RESP_TOKEN_HEADER, token)
        request.finish()

    def render_POST(self, request):
        """
        Start a match operation. Expects the query to be POSTed
        as the raw HTTP POST data.

        The query is a list of dictionaries. A dictionary should have the
        structure as defined in `vumi.persist.model.Model.index_match`

        The results of the query are stored fo limited time. It defaults
        to `MessageStoreCache.DEFAULT_SEARCH_RESULT_TTL` but can be overriden
        by specifying the TTL in seconds using the header key as specified
        in `REQ_TTL_HEADER`.

        If the request has the `REQ_WAIT_HEADER` value equals `1` (int)
        then it will only return with a response when the keys are actually
        available for collecting.
        """
        ttl = int(request.headers.get(self.REQ_TTL_HEADER, 0))
        query = json.loads(request.content.read())
        headers = request.requestHeaders
        if headers.hasHeader(self.REQ_WAIT_HEADER):
            wait = bool(int(headers.getRawHeaders(self.REQ_WAIT_HEADER)[0]))
        else:
            wait = False
        deferred = self._match_cb(query, ttl=(ttl or None), wait=wait)
        deferred.addCallback(self._render_token, request)
        return NOT_DONE_YET

    @inlineCallbacks
    def _render_keys(self, request, token, start, stop, asc):
        in_progress = yield self._in_progress_cb(token)
        count = yield self._count_cb(token)
        keys = yield self._results_cb(token, start, stop - 1, asc)
        request.responseHeaders.addRawHeader(self.RESP_IN_PROGRESS_HEADER,
            int(in_progress))
        request.responseHeaders.addRawHeader(self.RESP_COUNT_HEADER, count)
        request.write(json.dumps(keys))
        request.finish()

    def render_GET(self, request):
        token = request.args['token'][0]
        start = int(request.args['start'][0] if 'start' in request.args else 0)
        stop = int(request.args['stop'][0] if 'stop' in request.args
                    else self.DEFAULT_RESULT_SIZE)
        asc = bool(int(request.args['asc'][0]) if 'asc' in request.args
                    else False)
        self._render_keys(request, token, start, stop, asc)
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
