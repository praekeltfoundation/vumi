# -*- test-case-name: vumi.components.tests.test_message_store_resource -*-

from twisted.internet.defer import DeferredList
from twisted.web.resource import NoResource, Resource
from twisted.web.server import NOT_DONE_YET


# NOTE: Thanks Ned http://stackoverflow.com/a/312464!
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]


class MessageStoreProxyResource(Resource):

    isLeaf = True
    default_chunk_size = 10

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
        d = self.get_keys(self.message_store, self.batch_id)
        d.addCallback(lambda keys: chunks(keys, chunk_size))
        d.addCallback(self.handle_chunks, request)
        d.addCallback(lambda _: request.finish())
        return NOT_DONE_YET

    def get_keys(self, message_store, batch_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def get_message(self, message_store, message_id):
        raise NotImplementedError('To be implemented by sub-class.')

    def handle_chunks(self, chunks, request):
        return DeferredList([
            self.handle_chunk(chunk, request) for chunk in chunks])

    def handle_chunk(self, message_keys, request):
        d = self.get_messages(message_keys)
        d.addCallback(lambda results: [msg for success, msg in results])
        d.addCallback(self.write_messages, request)
        return d

    def get_messages(self, message_keys):
        return DeferredList([
            self.get_message(self.message_store, key)
            for key in message_keys])

    def write_messages(self, messages, request):
        for message in messages:
            request.write((u'%s\n' % (message.to_json(),)).encode('utf-8'))


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
