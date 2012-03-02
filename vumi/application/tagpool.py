# -*- test-case-name: vumi.application.tests.test_tagpool -*-
# -*- coding: utf-8 -*-

"""Tag pool manager."""


class TagpoolManager(object):

    def __init__(self, r_server, r_prefix):
        self.r_server = r_server
        self.r_prefix = r_prefix

    def acquire_tag(self, pool):
        local_tag = self._acquire_tag(pool)
        return (pool, local_tag) if local_tag is not None else None

    def release_tag(self, tag):
        pool, local_tag = tag
        self._release_tag(pool, local_tag)

    def declare_tags(self, tags):
        pools = {}
        for pool, local_tag in tags:
            pools.setdefault(pool, []).append(local_tag)
        for pool, local_tags in pools.items():
            self._declare_tags(pool, local_tags)

    def _tag_pool_keys(self, pool):
        return tuple(":".join([self.r_prefix, "tagpools", pool, state])
                     for state in ("free:list", "free:set", "inuse:set"))

    def _acquire_tag(self, pool):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        tag = self.r_server.lpop(free_list_key)
        if tag is not None:
            self.r_server.smove(free_set_key, inuse_set_key, tag)
        return tag

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
