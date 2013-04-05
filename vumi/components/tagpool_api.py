# -*- coding: utf-8 -*-

"""JSON RPC API for vumi.components.tagpool."""

from txjsonrpc.web.jsonrpc import JSONRPC
from txjsonrpc.jsonrpc import addIntrospection
from twisted.internet.defer import inlineCallbacks
from twisted.web.server import Site
from twisted.application import strports

from vumi.worker import BaseWorker
from vumi.config import ConfigDict, ConfigText
from vumi.persist.txredis_manager import TxRedisManager
from vumi.components.tagpool import TagpoolManager
from vumi.rpc import signature, Unicode, Tag, List, Dict


class TagpoolApiServer(JSONRPC):
    def __init__(self, tagpool):
        JSONRPC.__init__(self)
        self.tagpool = tagpool

    # TODO: Handle encoding pool and tag values to bytes better (or maybe
    # better, add suppport for unicode tag and pool names to TagpoolManager)?

    @signature(pool=Unicode("Name of pool to acquire tag from."),
               returns=Tag("Tag acquired (or None).", null=True))
    def jsonrpc_acquire_tag(self, pool):
        """Acquire a tag from the pool (returns None if no tags are avaliable).
           """
        return self.tagpool.acquire_tag(pool)

    @signature(tag=Tag("Tag to acquire as [pool, tagname] pair."),
               returns=Tag("Tag acquired (or None).", null=True))
    def jsonrpc_acquire_specific_tag(self, tag):
        """Acquire the specific tag (returns None if the tag is unavailable).
           """
        return self.tagpool.acquire_specific_tag(tag)

    @signature(tag=Tag("Tag to release."))
    def jsonrpc_release_tag(self, tag):
        """Release the specified tag if it exists and is inuse."""
        return self.tagpool.release_tag(tag)

    @signature(tags=List("List of tags to declare.", item_type=Tag()))
    def jsonrpc_declare_tags(self, tags):
        """Declare all of the listed tags."""
        return self.tagpool.declare_tags(tags)

    @signature(pool=Unicode("Name of pool to retreive metadata for."),
               returns=Dict("Retrieved metadata."))
    def jsonrpc_get_metadata(self, pool):
        """Retrieve the metadata for the given pool."""
        return self.tagpool.get_metadata(pool)

    @signature(pool=Unicode("Name of pool to update metadata for."),
               metadata=Dict("New value of metadata."))
    def jsonrpc_set_metadata(self, pool, metadata):
        """Set the metadata for the given pool."""
        return self.tagpool.set_metadata(pool, metadata)

    @signature(pool=Unicode("Name of the pool to purge."))
    def jsonrpc_purge_pool(self, pool):
        """Delete the given pool and all associated metadata and tags.

           No tags from the pool may be inuse.
           """
        return self.tagpool.purge_pool(pool)

    @signature(returns=List("List of pool names.", item_type=Unicode()))
    def jsonrpc_list_pools(self):
        """Return a list of all available pools."""
        d = self.tagpool.list_pools()
        d.addCallback(lambda l: [pool.decode('ascii') for pool in l])
        return d

    @signature(pool=Unicode("Name of pool."),
               returns=List("List of free tags.", item_type=Tag()))
    def jsonrpc_free_tags(self, pool):
        """Return a list of free tags in the given pool."""
        return self.tagpool.free_tags(pool)

    @signature(pool=Unicode("Name of pool."),
               returns=List("List of tags inuse.", item_type=Tag()))
    def jsonrpc_inuse_tags(self, pool):
        """Return a list of tags currently in use within the given pool."""
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
        rpc = TagpoolApiServer(tagpool)
        addIntrospection(rpc)
        site = Site(rpc)
        self.addService(strports.service(config.endpoint, site))


class TagpoolApiClient(object):
    pass
