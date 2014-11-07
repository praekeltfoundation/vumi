#!/usr/bin/env python
# -*- test-case-name: vumi.scripts.tests.test_vumi_tagpools -*-
import sys
import re
import itertools

import yaml
from twisted.python import usage

from vumi.components.tagpool import TagpoolManager
from vumi.persist.redis_manager import RedisManager


class PoolSubCmd(usage.Options):

    synopsis = "<pool>"

    def parseArgs(self, pool):
        self.pool = pool


class CreatePoolCmd(PoolSubCmd):
    def run(self, cfg):
        local_tags = cfg.tags(self.pool)
        tags = [(self.pool, local_tag) for local_tag in local_tags]
        metadata = cfg.metadata(self.pool)

        cfg.emit("Creating pool %s ..." % self.pool)
        cfg.emit("  Setting metadata ...")
        cfg.tagpool.set_metadata(self.pool, metadata)
        cfg.emit("  Declaring %d tag(s) ..." % len(tags))
        cfg.tagpool.declare_tags(tags)
        cfg.emit("  Done.")


class UpdatePoolMetadataCmd(PoolSubCmd):
    def run(self, cfg):
        metadata = cfg.metadata(self.pool)

        cfg.emit("Updating metadata for pool %s ..." % self.pool)
        cfg.tagpool.set_metadata(self.pool, metadata)
        cfg.emit("  Done.")


class UpdateAllPoolMetadataCmd(usage.Options):
    def run(self, cfg):
        pools_in_tagpool = cfg.tagpool.list_pools()
        pools_in_cfg = set(cfg.pools.keys())
        pools_in_both = sorted(pools_in_tagpool.intersection(pools_in_cfg))

        cfg.emit("Updating pool metadata.")
        cfg.emit("Note: Pools not present in both the config and tagpool"
                 " store will not be updated.")

        if not pools_in_both:
            cfg.emit("No pools found.")
            return

        for pool in pools_in_both:
            cfg.emit("  Updating metadata for pool %s ..." % pool)
            metadata = cfg.metadata(pool)
            cfg.tagpool.set_metadata(pool, metadata)

        cfg.emit("Done.")


class PurgePoolCmd(PoolSubCmd):
    def run(self, cfg):
        cfg.emit("Purging pool %s ..." % self.pool)
        cfg.tagpool.purge_pool(self.pool)
        cfg.emit("  Done.")


def key_ranges(keys):
    """Take a list of keys and convert them to a compact
    output string.

    E.g. foo100, foo101, ..., foo200, foo300
         becomes
         foo[100..200], foo300
    """
    keys.sort()
    last_digits_re = re.compile("^(?P<pre>()|(.*[^\d]))(?P<digits>\d+)"
                                "(?P<post>.*)$")

    def group(x):
        i, key = x
        match = last_digits_re.match(key)
        if not match:
            return None
        pre, post = match.group('pre'), match.group('post')
        digits = match.group('digits')
        dlen, value = len(digits), int(digits)
        return pre, post, dlen, value - i

    key_ranges = []
    for grp_key, grp_list in itertools.groupby(enumerate(keys), group):
        grp_list = list(grp_list)
        if len(grp_list) == 1 or grp_key is None:
            key_ranges.extend(g[1] for g in grp_list)
        else:
            pre, post, dlen, _cnt = grp_key
            start = last_digits_re.match(grp_list[0][1]).group('digits')
            end = last_digits_re.match(grp_list[-1][1]).group('digits')
            key_range = "%s[%s-%s]%s" % (pre, start, end, post)
            key_ranges.append(key_range)

    return ", ".join(key_ranges)


class ListKeysCmd(PoolSubCmd):
    def run(self, cfg):
        free_tags = cfg.tagpool.free_tags(self.pool)
        inuse_tags = cfg.tagpool.inuse_tags(self.pool)
        cfg.emit("Listing tags for pool %s ..." % self.pool)
        cfg.emit("Free tags:")
        cfg.emit("   " + (key_ranges([tag[1] for tag in free_tags])
                          or "-- None --"))
        cfg.emit("Tags in use:")
        cfg.emit("   " + (key_ranges([tag[1] for tag in inuse_tags])
                          or "-- None --"))


class ListPoolsCmd(usage.Options):
    def run(self, cfg):
        pools_in_tagpool = cfg.tagpool.list_pools()
        pools_in_cfg = set(cfg.pools.keys())
        cfg.emit("Pools defined in cfg and tagpool:")
        cfg.emit("   " +
                 ', '.join(sorted(pools_in_tagpool.intersection(pools_in_cfg))
                           or ['-- None --']))
        cfg.emit("Pools only in cfg:")
        cfg.emit("   " +
                 ', '.join(sorted(pools_in_cfg.difference(pools_in_tagpool))
                           or ['-- None --']))
        cfg.emit("Pools only in tagpool:")
        cfg.emit("   " +
                 ', '.join(sorted(pools_in_tagpool.difference(pools_in_cfg))
                           or ['-- None --']))


class ReleaseTagCmd(usage.Options):

    synopsis = "<pool> <tag>"

    def parseArgs(self, pool, tag):
        self.pool = pool
        self.tag = tag

    def run(self, cfg):
        free_tags = cfg.tagpool.free_tags(self.pool)
        inuse_tags = cfg.tagpool.inuse_tags(self.pool)
        tag_tuple = (self.pool, self.tag)
        if tag_tuple not in inuse_tags:
            if tag_tuple not in free_tags:
                cfg.emit('Unknown tag %s.' % (tag_tuple,))
            else:
                cfg.emit('Tag %s not in use.' % (tag_tuple,))
        else:
            cfg.tagpool.release_tag(tag_tuple)
            cfg.emit('Released %s.' % (tag_tuple,))


class Options(usage.Options):
    subCommands = [
        ["create-pool", None, CreatePoolCmd,
         "Declare tags for a tag pool."],
        ["update-pool-metadata", None, UpdatePoolMetadataCmd,
         "Update a pool's metadata from config."],
        ["update-all-metadata", None, UpdateAllPoolMetadataCmd,
         "Update all pool meta data from config."],
        ["purge-pool", None, PurgePoolCmd,
         "Purge all tags from a tag pool."],
        ["list-keys", None, ListKeysCmd,
         "List the free and inuse keys associated with a tag pool."],
        ["list-pools", None, ListPoolsCmd,
         "List all pools defined in config and in the tag store."],
        ["release-tag", None, ReleaseTagCmd,
         "Release a single tag, moves it from the in-use to the free set. "
         "Use only if you know what you are doing."]
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
        redis = RedisManager.from_config(self.config.get('redis_manager', {}))
        self.tagpool = TagpoolManager(redis.sub_manager(
                self.config.get('tagpool_prefix', 'vumi')))

    def emit(self, s):
        print s

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
