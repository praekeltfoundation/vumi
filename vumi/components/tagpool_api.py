# -*- coding: utf-8 -*-

"""JSON RPC API for vumi.components.tagpool."""

from txjsonrpc.web import jsonrpc


def signature(signature_for_f):
    """Define a JSON RPC method's signature."""
    def decorator(f):
        f.signature = [signature_for_f]
        return f
    return decorator


class TagpoolApiServer(jsonrpc.JSONRPC):
    def __init__(self, tagpool):
        super(TagpoolApiServer, self).__init__()
        self.tagpool = tagpool

    # TODO: add rest of signatures
    # TODO: add doc strings

    @signature(['tag', 'string'])
    def acquire_tag(self, pool):
        return self.tagpool.acquire(pool)

    @signature(['tag', 'tag'])
    def acquire_specific_tag(self, tag):
        return self.tagpool.acquire_specific_tag(tag)

    @signature(['null', 'tag'])
    def release_tag(self, tag):
        return self.tagpool.release_tag(tag)

    @signature(['null', 'array of tags'])
    def declare_tags(self, tags):
        return self.tagpool.declare_tags(tags)

    def get_metadata(self, pool):
        return self.tagpool.get_metadata(pool)

    def set_metadata(self, pool, metadata):
        return self.tagpool.get_metadata(pool, metadata)

    def purge_pool(self, pool):
        return self.tagpool.purge_pool(pool)

    def list_pools(self):
        return self.tagpool.list_pools()

    def free_tags(self, pool):
        return self.tagpool.free_tags(pool)

    def inuse_tags(self, pool):
        return self.tagpool.inuse_tags(pool)


class TagpoolApiWorker(Worker):

    class CONFIG_CLASS():


    def __init__(self):
        pass  # maybe put creation here?

    @inlineCallbacks
    def startWorker(self):
        tagpool = ...
        port = ...
        site = server.Site(TagpoolApiServer(tagpool))
        rpc_service = internet.TCPServer(port, site)
        self.addService(rpc_service)

    def stopWorker(self):
        pass


class TagpoolApiClient(object):
    pass
