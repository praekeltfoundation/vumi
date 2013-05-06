# -*- test-case-name: vumi.components.tests.test_tagpool -*-
# -*- coding: utf-8 -*-

"""Tag pool manager."""

import json
import time

from twisted.internet.defer import returnValue

from vumi.errors import VumiError
from vumi.persist.redis_base import Manager


class TagpoolError(VumiError):
    """An error occurred during an operation on a tag pool."""


class TagpoolManager(object):
    """Manage a set of tag pools.

    :param redis:
        An instance of :class:`vumi.persist.redis_base.Manager`.
    """

    encoding = "UTF-8"

    def __init__(self, redis):
        self.redis = redis
        self.manager = redis  # TODO: This is a bit of a hack to make the
                              #       the calls_manager decorator work

    def _encode(self, unicode_text):
        return unicode_text.encode(self.encoding)

    def _decode(self, binary_data):
        return binary_data.decode(self.encoding)

    @Manager.calls_manager
    def acquire_tag(self, pool, owner=None, reason=None):
        local_tag = yield self._acquire_tag(pool, owner, reason)
        returnValue((pool, local_tag) if local_tag is not None else None)

    @Manager.calls_manager
    def acquire_specific_tag(self, tag, owner=None, reason=None):
        pool, local_tag = tag
        acquired = yield self._acquire_specific_tag(pool, local_tag,
                                                    owner, reason)
        if acquired:
            returnValue(tag)
        returnValue(None)

    @Manager.calls_manager
    def release_tag(self, tag):
        pool, local_tag = tag
        yield self._release_tag(pool, local_tag)

    @Manager.calls_manager
    def declare_tags(self, tags):
        pools = {}
        for pool, local_tag in tags:
            pools.setdefault(pool, []).append(local_tag)
        for pool, local_tags in pools.items():
            yield self._register_pool(pool)
            yield self._declare_tags(pool, local_tags)

    @Manager.calls_manager
    def get_metadata(self, pool):
        metadata_key = self._tag_pool_metadata_key(pool)
        metadata = yield self.redis.hgetall(metadata_key)
        metadata = dict((self._decode(k), json.loads(v))
                        for k, v in metadata.iteritems())
        returnValue(metadata)

    @Manager.calls_manager
    def set_metadata(self, pool, metadata):
        metadata_key = self._tag_pool_metadata_key(pool)
        metadata = dict((self._encode(k), json.dumps(v))
                        for k, v in metadata.iteritems())
        yield self._register_pool(pool)
        yield self.redis.delete(metadata_key)
        yield self.redis.hmset(metadata_key, metadata)

    @Manager.calls_manager
    def purge_pool(self, pool):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        metadata_key = self._tag_pool_metadata_key(pool)
        in_use_count = yield self.redis.scard(inuse_set_key)
        if in_use_count:
            raise TagpoolError('%s tags of pool %s still in use.' % (
                               in_use_count, pool))
        else:
            yield self.redis.delete(free_set_key)
            yield self.redis.delete(free_list_key)
            yield self.redis.delete(inuse_set_key)
            yield self.redis.delete(metadata_key)
            yield self._unregister_pool(pool)

    @Manager.calls_manager
    def list_pools(self):
        pool_list_key = self._pool_list_key()
        pools = yield self.redis.smembers(pool_list_key)
        returnValue(set(self._decode(pool) for pool in pools))

    @Manager.calls_manager
    def free_tags(self, pool):
        _free_list, free_set_key, _inuse_set = self._tag_pool_keys(pool)
        free_tags = yield self.redis.smembers(free_set_key)
        returnValue([(pool, self._decode(local_tag))
                     for local_tag in free_tags])

    @Manager.calls_manager
    def inuse_tags(self, pool):
        _free_list, _free_set, inuse_set_key = self._tag_pool_keys(pool)
        inuse_tags = yield self.redis.smembers(inuse_set_key)
        returnValue([(pool, self._decode(local_tag))
                     for local_tag in inuse_tags])

    @Manager.calls_manager
    def acquired_by(self, tag):
        pool, local_tag = tag
        local_tag = self._encode(local_tag)
        reason_hash_key = self._tag_pool_reason_key(pool)
        raw_reason = yield self.redis.hget(reason_hash_key, local_tag)
        if raw_reason is not None:
            reason = json.loads(raw_reason)
            owner = reason.get('owner')
        else:
            reason, owner = None, None
        returnValue((owner, reason))

    @Manager.calls_manager
    def owned_tags(self, owner):
        owner_tag_list_key = self._owner_tag_list_key(owner)
        owned_tags = yield self.redis.smembers(owner_tag_list_key)
        returnValue([json.loads(raw_tag) for raw_tag in owned_tags])

    def _pool_list_key(self):
        return ":".join(["tagpools", "list"])

    @Manager.calls_manager
    def _register_pool(self, pool):
        """Add a pool to list of pools."""
        pool = self._encode(pool)
        pool_list_key = self._pool_list_key()
        yield self.redis.sadd(pool_list_key, pool)

    @Manager.calls_manager
    def _unregister_pool(self, pool):
        """Remove a pool to list of pools."""
        pool = self._encode(pool)
        pool_list_key = self._pool_list_key()
        yield self.redis.srem(pool_list_key, pool)

    def _tag_pool_keys(self, pool):
        pool = self._encode(pool)
        return tuple(":".join(["tagpools", pool, state])
                     for state in ("free:list", "free:set", "inuse:set"))

    def _tag_pool_metadata_key(self, pool):
        pool = self._encode(pool)
        return ":".join(["tagpools", pool, "metadata"])

    @Manager.calls_manager
    def _acquire_tag(self, pool, owner, reason):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        tag = yield self.redis.lpop(free_list_key)
        if tag is not None:
            yield self.redis.smove(free_set_key, inuse_set_key, tag)
            yield self._store_reason(pool, tag, owner, reason)
        returnValue(self._decode(tag) if tag is not None else None)

    @Manager.calls_manager
    def _acquire_specific_tag(self, pool, local_tag, owner, reason):
        local_tag = self._encode(local_tag)
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        moved = yield self.redis.lrem(free_list_key, local_tag, num=1)
        if moved:
            yield self.redis.smove(free_set_key, inuse_set_key, local_tag)
            yield self._store_reason(pool, local_tag, owner, reason)
        returnValue(moved)

    @Manager.calls_manager
    def _release_tag(self, pool, local_tag):
        local_tag = self._encode(local_tag)
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        count = yield self.redis.smove(inuse_set_key, free_set_key, local_tag)
        if count == 1:
            yield self.redis.rpush(free_list_key, local_tag)
            yield self._remove_reason(pool, local_tag)

    @Manager.calls_manager
    def _declare_tags(self, pool, local_tags):
        free_list_key, free_set_key, inuse_set_key = self._tag_pool_keys(pool)
        new_tags = set(self._encode(tag) for tag in local_tags)
        old_tags = yield self.redis.sunion(free_set_key, inuse_set_key)
        old_tags = set(old_tags)
        for tag in sorted(new_tags - old_tags):
            yield self.redis.sadd(free_set_key, tag)
            yield self.redis.rpush(free_list_key, tag)

    def _tag_pool_reason_key(self, pool):
        pool = self._encode(pool)
        return ":".join(["tagpools", pool, "reason:hash"])

    def _owner_tag_list_key(self, owner):
        if owner is None:
            return ":".join(["tagpools", "unowned", "tags"])
        owner = self._encode(owner)
        return ":".join(["tagpools", "owners", owner, "tags"])

    @Manager.calls_manager
    def _store_reason(self, pool, local_tag, owner, reason):
        if reason is None:
            reason = {}
        reason['timestamp'] = time.time()
        reason['owner'] = owner
        reason_hash_key = self._tag_pool_reason_key(pool)
        yield self.redis.hset(reason_hash_key, local_tag, json.dumps(reason))
        owner_tag_list_key = self._owner_tag_list_key(owner)
        yield self.redis.sadd(owner_tag_list_key,
                              json.dumps([pool, self._decode(local_tag)]))

    @Manager.calls_manager
    def _remove_reason(self, pool, local_tag):
        reason_hash_key = self._tag_pool_reason_key(pool)
        reason = yield self.redis.hget(reason_hash_key, local_tag)
        if reason is not None:
            reason = json.loads(reason)
            owner = reason.get('owner')
            owner_tag_list_key = self._owner_tag_list_key(owner)
            self.redis.srem(owner_tag_list_key,
                            json.dumps([pool, self._decode(local_tag)]))
