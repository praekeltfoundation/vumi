import sys

from twisted.internet.defer import DeferredList, inlineCallbacks


class MessageStoreExporterException(Exception):
    pass


# NOTE: Thanks Ned http://stackoverflow.com/a/312464!
def make_chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i + n]


class MessageStoreExporter(object):

    def __init__(self, chunk_size=10, concurrency=10):
        self.chunk_size = chunk_size
        self.concurrency = concurrency

    def export(self, keys, cb):
        chunks = make_chunks(keys, self.chunk_size)
        return self.fetch_chunks(list(chunks), self.concurrency, cb)

    @inlineCallbacks
    def fetch_chunks(self, chunked_keys, concurrency, cb):
        while chunked_keys:
            block, chunked_keys = (
                chunked_keys[:concurrency], chunked_keys[concurrency:])
            yield self.handle_chunks(block, cb)

    def handle_chunks(self, chunks, cb):
        return DeferredList([
            self.handle_chunk(chunk, cb) for chunk in chunks])

    def handle_chunk(self, message_keys, cb):
        return DeferredList([
            cb(key) for key in message_keys])
