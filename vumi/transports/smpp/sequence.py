# -*- test-case-name: vumi.transports.smpp.tests.test_sequence -*-
from twisted.internet.defer import inlineCallbacks, returnValue


class RedisSequence(object):

    """
    Generate a sequence of incrementing numbers that rollover
    at a given limit.

    This is backed by Redis' atomicity and safe to use in a
    distributed system.
    """

    def __init__(self, redis, rollover_at=0xFFFF0000):
        self.redis = redis
        self.rollover_at = rollover_at

    def __iter__(self):
        return self

    def next(self):
        return self.get_next_seq()

    @inlineCallbacks
    def get_next_seq(self):
        """Get the next available SMPP sequence number.

        The valid range of sequence number is 0x00000001 to 0xFFFFFFFF.

        We start trying to wrap at 0xFFFF0000 so we can keep returning values
        (up to 0xFFFF of them) even while someone else is in the middle of
        resetting the counter.
        """
        seq = yield self.redis.incr('smpp_last_sequence_number')

        if seq >= self.rollover_at:
            # We're close to the upper limit, so try to reset. It doesn't
            # matter if we actually succeed or not, since we're going to return
            # `seq` anyway.
            yield self._reset_seq_counter()

        returnValue(seq)

    @inlineCallbacks
    def _reset_seq_counter(self):
        """Reset the sequence counter in a safe manner.

        NOTE: There is a potential race condition in this implementation. If we
        acquire the lock and it expires while we still think we hold it, it's
        possible for the sequence number to be reset by someone else between
        the final vlue check and the reset call. This seems like a very
        unlikely situation, so we'll leave it like that for now.

        A better solution is to replace this whole method with a lua script
        that we send to redis, but scripting support is still very new at the
        time of writing.
        """
        # SETNX can be used as a lock.
        locked = yield self.redis.setnx('smpp_last_sequence_number_wrap', 1)

        # If someone crashed in exactly the wrong place, the lock may be
        # held by someone else but have no expire time. A race condition
        # here may set the TTL multiple times, but that's fine.
        if (yield self.redis.ttl('smpp_last_sequence_number_wrap')) < 0:
            # The TTL only gets set if the lock exists and recently had no TTL.
            yield self.redis.expire('smpp_last_sequence_number_wrap', 10)

        if not locked:
            # We didn't actually get the lock, so our job is done.
            return

        if (yield self.redis.get('smpp_last_sequence_number')) < 0xFFFF0000:
            # Our stored sequence number is no longer outside the allowed
            # range, so someone else must have reset it before we got the lock.
            return

        # We reset the counter by deleting the key. The next INCR will recreate
        # it for us.
        yield self.redis.delete('smpp_last_sequence_number')
