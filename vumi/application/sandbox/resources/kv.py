from __future__ import absolute_import

import logging
import json

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.application.sandbox.resources.utils import SandboxResource
from vumi.persist.txredis_manager import TxRedisManager


class RedisResource(SandboxResource):
    """
    Resource that provides access to a simple key-value store.

    Configuration options:

    :param dict redis_manager:
        Redis manager configuration options.
    :param int keys_per_user_soft:
        Maximum number of keys each user may make use of in redis
        before usage warnings are logged.
        (default: 80% of hard limit).
    :param int keys_per_user_hard:
        Maximum number of keys each user may make use of in redis
        (default: 100). Falls back to keys_per_user.
    :param int keys_per_user:
        Synonym for `keys_per_user_hard`. Deprecated.
    """

    # FIXME:
    #  - Currently we allow key expiry to be set. Keys that expire are
    #    not decremented from the sandbox's key limit. This means that
    #    some sandboxes might hit their key limit too soon. This is
    #    better than not allowing expiry of keys and filling up Redis
    #    though.

    @inlineCallbacks
    def setup(self):
        self.r_config = self.config.get('redis_manager', {})
        self.keys_per_user_hard = self.config.get(
            'keys_per_user_hard', self.config.get('keys_per_user', 100))
        self.keys_per_user_soft = self.config.get(
            'keys_per_user_soft', int(0.8 * self.keys_per_user_hard))
        self.redis = yield TxRedisManager.from_config(self.r_config)

    def teardown(self):
        return self.redis.close_manager()

    def _count_key(self, sandbox_id):
        return "#".join(["count", sandbox_id])

    def _sandboxed_key(self, sandbox_id, key):
        return "#".join(["sandboxes", sandbox_id, key])

    def _too_many_keys(self, command):
        return self.reply(command, success=False,
                          reason="Too many keys")

    @inlineCallbacks
    def check_keys(self, api, key):
        if (yield self.redis.exists(key)):
            returnValue(True)
        count_key = self._count_key(api.sandbox_id)
        key_count = yield self.redis.incr(count_key, 1)
        if key_count > self.keys_per_user_soft:
            if key_count < self.keys_per_user_hard:
                api.log('Redis soft limit of %s keys reached for sandbox %s. '
                        'Once the hard limit of %s is reached no more keys '
                        'can be written.' % (
                            self.keys_per_user_soft,
                            api.sandbox_id,
                            self.keys_per_user_hard),
                        logging.WARNING)
            else:
                api.log('Redis hard limit of %s keys reached for sandbox %s. '
                        'No more keys can be written.' % (
                            self.keys_per_user_hard,
                            api.sandbox_id),
                        logging.ERROR)
                yield self.redis.incr(count_key, -1)
                returnValue(False)
        returnValue(True)

    @inlineCallbacks
    def handle_set(self, api, command):
        """
        Set the value of a key.

        Command fields:
            - ``key``: The key whose value should be set.
            - ``value``: The value to store. May be any JSON serializable
              object.
            - ``seconds``: Lifetime of the key in seconds. The default ``null``
              indicates that the key should not expire.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.

        Example:

        .. code-block:: javascript

            api.request(
                'kv.set',
                {key: 'foo',
                 value: {x: '42'}},
                function(reply) { api.log_info('Value store: ' +
                                               reply.success); });
        """
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        seconds = command.get('seconds')
        if not (seconds is None or isinstance(seconds, (int, long))):
            returnValue(self.reply_error(
                command, "seconds must be a number or null"))
        if not (yield self.check_keys(api, key)):
            returnValue(self._too_many_keys(command))
        json_value = json.dumps(command.get('value'))
        if seconds is None:
            yield self.redis.set(key, json_value)
        else:
            yield self.redis.setex(key, seconds, json_value)
        returnValue(self.reply(command, success=True))

    @inlineCallbacks
    def handle_get(self, api, command):
        """
        Retrieve the value of a key.

        Command fields:
            - ``key``: The key whose value should be retrieved.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.
            - ``value``: The value retrieved.

        Example:

        .. code-block:: javascript

            api.request(
                'kv.get',
                {key: 'foo'},
                function(reply) {
                    api.log_info(
                        'Value retrieved: ' +
                        JSON.stringify(reply.value));
                }
            );
        """
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        raw_value = yield self.redis.get(key)
        value = json.loads(raw_value) if raw_value is not None else None
        returnValue(self.reply(command, success=True,
                               value=value))

    @inlineCallbacks
    def handle_delete(self, api, command):
        """
        Delete a key.

        Command fields:
            - ``key``: The key to delete.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.

        Example:

        .. code-block:: javascript

            api.request(
                'kv.delete',
                {key: 'foo'},
                function(reply) {
                    api.log_info('Value deleted: ' +
                                 reply.success);
                }
            );
        """
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        existed = bool((yield self.redis.delete(key)))
        if existed:
            count_key = self._count_key(api.sandbox_id)
            yield self.redis.incr(count_key, -1)
        returnValue(self.reply(command, success=True,
                               existed=existed))

    @inlineCallbacks
    def handle_incr(self, api, command):
        """
        Atomically increment the value of an integer key.

        The current value of the key must be an integer. If the key does not
        exist, it is set to zero.

        Command fields:
            - ``key``: The key to delete.
            - ``amount``: The integer amount to increment the key by. Defaults
              to 1.

        Reply fields:
            - ``success``: ``true`` if the operation was successful, otherwise
              ``false``.
            - ``value``: The new value of the key.

        Example:

        .. code-block:: javascript

            api.request(
                'kv.incr',
                {key: 'foo',
                 amount: 3},
                function(reply) {
                    api.log_info('New value: ' +
                                 reply.value);
                }
            );
        """
        key = self._sandboxed_key(api.sandbox_id, command.get('key'))
        if not (yield self.check_keys(api, key)):
            returnValue(self._too_many_keys(command))
        amount = command.get('amount', 1)
        try:
            value = yield self.redis.incr(key, amount=amount)
        except Exception, e:
            returnValue(self.reply(command, success=False, reason=unicode(e)))
        returnValue(self.reply(command, value=int(value), success=True))
