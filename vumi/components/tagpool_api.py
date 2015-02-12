# -*- coding: utf-8 -*-

"""JSON RPC API for vumi.components.tagpool."""

from txjsonrpc.web.jsonrpc import JSONRPC
from txjsonrpc.jsonrpc import addIntrospection

from twisted.application.internet import StreamServerEndpointService
from twisted.internet.defer import inlineCallbacks

from vumi.worker import BaseWorker
from vumi.config import (
    ConfigDict, ConfigText, ConfigServerEndpoint, ConfigInt,
    ServerEndpointFallback)
from vumi.persist.txredis_manager import TxRedisManager
from vumi.components.tagpool import TagpoolManager
from vumi.rpc import signature, Unicode, Tag, List, Dict
from vumi.transports.httprpc import httprpc
from vumi.utils import build_web_site


class TagpoolApiServer(JSONRPC):
    def __init__(self, tagpool):
        JSONRPC.__init__(self)
        self.tagpool = tagpool

    @signature(pool=Unicode("Name of pool to acquire tag from."),
               owner=Unicode("Owner acquiring tag (or None).", null=True),
               reason=Dict("Metadata on why tag is being acquired (or None).",
                           null=True),
               returns=Tag("Tag acquired (or None).", null=True))
    def jsonrpc_acquire_tag(self, pool, owner=None, reason=None):
        """Acquire a tag from the pool (returns None if no tags are avaliable).
           """
        d = self.tagpool.acquire_tag(pool, owner, reason)
        return d

    @signature(tag=Tag("Tag to acquire as [pool, tagname] pair."),
               owner=Unicode("Owner acquiring tag (or None).", null=True),
               reason=Dict("Metadata on why tag is being acquired (or None).",
                           null=True),
               returns=Tag("Tag acquired (or None).", null=True))
    def jsonrpc_acquire_specific_tag(self, tag, owner=None, reason=None):
        """Acquire the specific tag (returns None if the tag is unavailable).
           """
        d = self.tagpool.acquire_specific_tag(tag, owner, reason)
        return d

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
        d.addCallback(list)
        return d

    @signature(pool=Unicode("Name of pool."),
               returns=List("List of free tags.", item_type=Tag()))
    def jsonrpc_free_tags(self, pool):
        """Return a list of free tags in the given pool."""
        d = self.tagpool.free_tags(pool)
        return d

    @signature(pool=Unicode("Name of pool."),
               returns=List("List of tags inuse.", item_type=Tag()))
    def jsonrpc_inuse_tags(self, pool):
        """Return a list of tags currently in use within the given pool."""
        d = self.tagpool.inuse_tags(pool)
        return d

    @signature(tag=Tag("Tag to return ownership information on."),
               returns=List("List of owner and reason.", length=2, null=True))
    def jsonrpc_acquired_by(self, tag):
        """Returns the owner of an acquired tag and why is was acquired."""
        d = self.tagpool.acquired_by(tag)
        d.addCallback(list)
        return d

    @signature(owner=Unicode("Owner of tags (or None for unowned tags).",
                             null=True),
               returns=List("List of tags owned.", item_type=Tag()))
    def jsonrpc_owned_tags(self, owner):
        """Return a list of tags currently owned by an owner."""
        return self.tagpool.owned_tags(owner)


class TagpoolApiWorker(BaseWorker):

    class CONFIG_CLASS(BaseWorker.CONFIG_CLASS):
        worker_name = ConfigText(
            "Name of this tagpool API worker.", required=True, static=True)
        twisted_endpoint = ConfigServerEndpoint(
            "Twisted endpoint to listen on.", required=True, static=True,
            fallbacks=[ServerEndpointFallback()])
        web_path = ConfigText(
            "The path to serve this resource on.", required=True, static=True)
        health_path = ConfigText(
            "The path to server the health resource on.", default='/health/',
            static=True)
        redis_manager = ConfigDict(
            "Redis client configuration.", default={}, static=True)

        # TODO: Deprecate these fields when confmodel#5 is done.
        host = ConfigText(
            "*DEPRECATED* 'host' and 'port' fields may be used in place of"
            " the 'twisted_endpoint' field.", static=True)
        port = ConfigInt(
            "*DEPRECATED* 'host' and 'port' fields may be used in place of"
            " the 'twisted_endpoint' field.", static=True)

    def get_health_response(self):
        return "OK"

    @inlineCallbacks
    def setup_worker(self):
        config = self.get_static_config()
        self.redis_manager = yield TxRedisManager.from_config(
            config.redis_manager)
        tagpool = TagpoolManager(self.redis_manager)
        rpc = TagpoolApiServer(tagpool)
        addIntrospection(rpc)
        site = build_web_site({
            config.web_path: rpc,
            config.health_path: httprpc.HttpRpcHealthResource(self),
        })
        self.addService(
            StreamServerEndpointService(config.twisted_endpoint, site))

    def teardown_worker(self):
        pass

    def setup_connectors(self):
        pass
