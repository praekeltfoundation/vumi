# -*- test-case-name: vumi.application.tests.test_tagpool -*-
# -*- coding: utf-8 -*-

"""Tag pool manager."""

import json

from vumi.errors import VumiError


class TagpoolError(VumiError):
    """An error occurred during an operation on a tag pool."""


class TagpoolManager(object):

    def __init__(self, r_server, r_prefix):
        self.r_server = r_server
        self.r_prefix = r_prefix

    def acquire_tag(self, pool):
        local_tag = self._acquire_tag(pool)
        return (pool, local_tag) if local_tag is not None else None

    def acquire_specific_tag(self, tag):
        pool, local_tag = tag
        if self._acquire_specific_tag(pool, local_tag):
            return tag
        return None

    def release_tag(self, tag):
        pool, local_tag = tag
        self._release_tag(pool, local_tag)

    def declare_tags(self, tags):
        pools = {}
        for pool, local_tag in tags:
            pools.setdefault(pool, []).append(local_tag)
        for pool, local_tags in pools.items():
            self._declare_tags(pool, local_tags)

    def get_metadata(self, pool):
        metadata_key = self._tag_pool_metadata_key(pool)
        metadata = self.r_server.hgetall(metadata_key)
        metadata = dict((k, json.loads(v)) for k, v in metadata.iteritems())
        return metadata

    def set_metadata(self, pool, metadata):
        metadata_key = self._tag_pool_metadata_key(pool)
        metadata = dict((k, json.dumps(v)) for k, v in metadata.iteritems())
        self.r_server.delete(metadata_key)
        self.r_server.hmset(metadata_key, metadata)

    def purge_pool(self, pool):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        metadata_key = self._tag_pool_metadata_key(pool)
        in_use_count = self.r_server.scard(inuse_set_key)
        if in_use_count:
            raise TagpoolError('%s tags of pool %s still in use.' % (
                                in_use_count, pool))
        else:
            self.r_server.delete(free_set_key)
            self.r_server.delete(free_list_key)
            self.r_server.delete(inuse_set_key)
            self.r_server.delete(metadata_key)

    def list_pools(self):
        keys = self.r_server.keys("%s:tagpools:*:free:set" % (self.r_prefix,))
        # split from the right because r_prefix might contain a colon.
        return [key.rsplit(":", 3)[-3] for key in keys]

    def free_tags(self, pool):
        _free_list, free_set_key, _inuse_set = self._tag_pool_keys(pool)
        return [(pool, local_tag) for local_tag in
                self.r_server.smembers(free_set_key)]

    def inuse_tags(self, pool):
        _free_list, _free_set, inuse_set_key = self._tag_pool_keys(pool)
        return [(pool, local_tag) for local_tag in
                self.r_server.smembers(inuse_set_key)]

    def _tag_pool_keys(self, pool):
        return tuple(":".join([self.r_prefix, "tagpools", pool, state])
                     for state in ("free:list", "free:set", "inuse:set"))

    def _tag_pool_metadata_key(self, pool):
        return ":".join([self.r_prefix, "tagpools", pool, "metadata"])

    def _acquire_tag(self, pool):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        tag = self.r_server.lpop(free_list_key)
        if tag is not None:
            self.r_server.smove(free_set_key, inuse_set_key, tag)
        return tag

    def _acquire_specific_tag(self, pool, local_tag):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        moved = self.r_server.smove(free_set_key, inuse_set_key, local_tag)
        if moved:
            self.r_server.lrem(free_list_key, local_tag, num=1)
        return moved

    def _release_tag(self, pool, local_tag):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        count = self.r_server.smove(inuse_set_key, free_set_key, local_tag)
        if count == 1:
            self.r_server.rpush(free_list_key, local_tag)

    def _declare_tags(self, pool, local_tags):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        new_tags = set(local_tags)
        old_tags = set(self.r_server.sunion(free_set_key, inuse_set_key))
        for tag in sorted(new_tags - old_tags):
            self.r_server.sadd(free_set_key, tag)
            self.r_server.rpush(free_list_key, tag)
