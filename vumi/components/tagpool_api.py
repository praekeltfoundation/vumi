# -*- coding: utf-8 -*-

"""JSON RPC API for vumi.components.tagpool."""

from txjsonrpc.web.jsonrpc import JSONRPC
from twisted.internet.defer import inlineCallbacks
from twisted.web.server import Site
from twisted.application import strports

from vumi.worker import BaseWorker
from vumi.config import ConfigDict, ConfigText
from vumi.persist.txredis_manager import TxRedisManager
from vumi.components.tagpool import TagpoolManager


def signature(signature_for_f):
    """Define a JSON RPC method's signature."""
    def decorator(f):
        f.signature = [signature_for_f]
        return f
    return decorator


class TagpoolApiServer(JSONRPC):
    def __init__(self, tagpool):
        JSONRPC.__init__(self)
        self.tagpool = tagpool

    # TODO: add rest of signatures
    # TODO: add doc strings

    @signature(['tag', 'string'])
    def jsonrpc_acquire_tag(self, pool):
        return self.tagpool.acquire_tag(pool)

    @signature(['tag', 'tag'])
    def jsonrpc_acquire_specific_tag(self, tag):
        return self.tagpool.acquire_specific_tag(tag)

    @signature(['null', 'tag'])
    def jsonrpc_release_tag(self, tag):
        return self.tagpool.release_tag(tag)

    @signature(['null', 'array of tags'])
    def jsonrpc_declare_tags(self, tags):
        return self.tagpool.declare_tags(tags)

    def jsonrpc_get_metadata(self, pool):
        return self.tagpool.get_metadata(pool)

    def jsonrpc_set_metadata(self, pool, metadata):
        return self.tagpool.set_metadata(pool, metadata)

    def jsonrpc_purge_pool(self, pool):
        return self.tagpool.purge_pool(pool)

    def jsonrpc_list_pools(self):
        d = self.tagpool.list_pools()
        d.addCallback(list)
        return d

    def jsonrpc_free_tags(self, pool):
        return self.tagpool.free_tags(pool)

    def jsonrpc_inuse_tags(self, pool):
        return self.tagpool.inuse_tags(pool)


class TagpoolApiWorker(BaseWorker):

    class CONFIG_CLASS(BaseWorker.CONFIG_CLASS):
        worker_name = ConfigText(
            "Name of this tagpool API worker.", required=True, static=True)
        endpoint = ConfigText(
            "Endpoint to listen on.", required=True, static=True)
        redis_manager = ConfigDict(
            "Redis client configuration.", default={}, static=True)

    @inlineCallbacks
    def startWorker(self):
        config = self.get_static_config()
        redis_manager = yield TxRedisManager.from_config(config.redis_manager)
        tagpool = TagpoolManager(redis_manager)
        site = Site(TagpoolApiServer(tagpool))
        self.addService(strports.service(config.endpoint, site))


class TagpoolApiClient(object):
    pass
