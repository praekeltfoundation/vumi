# -*- test-case-name: vumi.scripts.tests.test_vumi_tagpools -*-
import sys

import yaml
import redis
from twisted.python import usage

from vumi.application import TagpoolManager


class PoolSubCmd(usage.Options):

    synopsis = "<pool>"

    def parseArgs(self, pool):
        self.pool = pool


class CreatePoolCmd(PoolSubCmd):
    def run(self, cfg):
        local_tags = cfg.tags(self.pool)
        tags = [(self.pool, local_tag) for local_tag in local_tags]
        metadata = cfg.metadata(self.pool)

        print "Creating pool %s ..." % self.pool
        print "  Setting metadata ..."
        print cfg.tagpool.set_metadata(self.pool, metadata)
        print "  Declaring tags %d tags ..." % len(tags)
        print cfg.tagpool.declare_tags(tags)
        print "  Done."


class PurgePoolCmd(PoolSubCmd):
    def run(self, cfg):
        print "Purging pool %s ..." % self.pool
        print cfg.tagpool.purge_pool(self.pool)
        print "  Done."


class ListPoolsCmd(usage.Options):
    def run(self, cfg):
        pools_in_tagpool = set(cfg.tagpool.list_pools())
        pools_in_cfg = set(cfg.pools.keys())
        print "Pools defined in cfg and tagpool ..."
        print "  ", ', '.join(pools_in_tagpool.union(pools_in_cfg))
        print "Pools in cfg ..."
        print "  ", ', '.join(pools_in_cfg)
        print "Pools in tagpool ..."
        print "  ", ', '.join(pools_in_tagpool)
        print "New pools ..."
        print "  ", ', '.join(pools_in_cfg.difference(pools_in_tagpool))


class Options(usage.Options):
    subCommands = [
        ["create-pool", None, CreatePoolCmd,
         "Declare tags for a tag pool."],
        ["purge-pool", None, PurgePoolCmd,
         "Purge all tags from a tag pool."],
        ["list-pools", None, ListPoolsCmd,
         "List all pools defined in config and in the tag store."],
        ]

    optParameters = [
        ["config", "c", "tagpools.yaml",
         "A config file describing the available pools."],
    ]

    longdesc = """Utilities for working with
                  vumi.application.TagPoolManager."""

    def postOptions(self):
        if self.subCommand is None:
            raise usage.UsageError("Please specify a sub-command.")


class ConfigHolder(object):
    def __init__(self, options):
        self.options = options
        self.config = yaml.safe_load(open(options['config'], "rb"))
        self.pools = self.config.get('pools', {})
        r_server = redis.Redis(**self.config.get('redis', {}))
        r_prefix = self.config.get('r_prefix', 'vumi')
        self.tagpool = TagpoolManager(r_server, r_prefix)

    def tags(self, pool):
        tags = self.pools[pool]['tags']
        if isinstance(tags, basestring):
            tags = eval(tags, {}, {})
        return tags

    def metadata(self, pool):
        return self.pools[pool].get('metadata', {})

    def run(self):
        self.options.subOptions.run(self)


if __name__ == '__main__':
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    cfg = ConfigHolder(options)
    cfg.run()
