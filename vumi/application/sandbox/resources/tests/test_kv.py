import json
import logging

from twisted.internet.defer import inlineCallbacks

from vumi.application.sandbox.resources.kv import RedisResource
from vumi.tests.helpers import PersistenceHelper
from vumi.application.sandbox.resources.tests.utils import (
    ResourceTestCaseBase)


class TestRedisResource(ResourceTestCaseBase):

    resource_cls = RedisResource

    @inlineCallbacks
    def setUp(self):
        super(TestRedisResource, self).setUp()
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.r_server = yield self.persistence_helper.get_redis_manager()
        yield self.create_resource({})

    def create_resource(self, config):
        config.setdefault('redis_manager', {
            'FAKE_REDIS': self.r_server,
            'key_prefix': self.r_server._key_prefix,
        })
        return super(TestRedisResource, self).create_resource(config)

    @inlineCallbacks
    def create_metric(self, metric, value, total_count=1):
        metric_key = 'sandboxes#test_id#' + metric
        count_key = 'count#test_id'
        yield self.r_server.set(metric_key, value)
        yield self.r_server.set(count_key, total_count)

    @inlineCallbacks
    def check_metric(self, metric, value, total_count, seconds=None):
        metric_key = 'sandboxes#test_id#' + metric
        count_key = 'count#test_id'
        self.assertEqual((yield self.r_server.get(metric_key)), value)
        self.assertEqual((yield self.r_server.get(count_key)),
                         str(total_count) if total_count is not None else None)
        ttl = yield self.r_server.ttl(metric_key)
        if seconds is None:
            self.assertEqual(ttl, None)
        else:
            self.assertNotEqual(ttl, None)
            self.assertTrue(0 < ttl <= seconds)

    def assert_api_log(self, expected_level, expected_message):
        [log_entry] = self.api.logs
        level, message = log_entry
        self.assertEqual(level, expected_level)
        self.assertEqual(message, expected_message)

    @inlineCallbacks
    def test_handle_set(self):
        reply = yield self.dispatch_command('set', key='foo', value='bar')
        self.check_reply(reply, success=True)
        yield self.check_metric('foo', json.dumps('bar'), 1)

    @inlineCallbacks
    def test_handle_set_with_expiry(self):
        reply = yield self.dispatch_command(
            'set', key='foo', value='bar', seconds=5)
        self.check_reply(reply, success=True)
        yield self.check_metric('foo', json.dumps('bar'), 1, seconds=5)

    @inlineCallbacks
    def test_handle_set_with_bad_seconds(self):
        reply = yield self.dispatch_command(
            'set', key='foo', value='bar', seconds='foo')
        self.check_reply(
            reply, success=False,
            reason="seconds must be a number or null")
        yield self.check_metric('foo', None, None)

    @inlineCallbacks
    def test_handle_set_soft_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=80)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=True)
        self.assert_api_log(
            logging.WARNING,
            'Redis soft limit of 80 keys reached for sandbox test_id. '
            'Once the hard limit of 100 is reached no more keys can '
            'be written.'
        )

    @inlineCallbacks
    def test_handle_set_hard_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=100)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=False, reason='Too many keys')
        yield self.check_metric('bar', None, 100)
        self.assert_api_log(
            logging.ERROR,
            'Redis hard limit of 100 keys reached for sandbox test_id. '
            'No more keys can be written.'
        )

    @inlineCallbacks
    def test_keys_per_user_fallback_hard_limit(self):
        yield self.create_resource({
            'keys_per_user': 10,
        })
        yield self.create_metric('foo', 'a', total_count=10)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=False, reason='Too many keys')
        self.assert_api_log(
            logging.ERROR,
            'Redis hard limit of 10 keys reached for sandbox test_id. '
            'No more keys can be written.'
        )

    @inlineCallbacks
    def test_keys_per_user_fallback_soft_limit(self):
        yield self.create_resource({
            'keys_per_user': 10,
        })
        yield self.create_metric('foo', 'a', total_count=8)
        reply = yield self.dispatch_command('set', key='bar', value='bar')
        self.check_reply(reply, success=True)
        self.assert_api_log(
            logging.WARNING,
            'Redis soft limit of 8 keys reached for sandbox test_id. '
            'Once the hard limit of 10 is reached no more keys can '
            'be written.'
        )

    @inlineCallbacks
    def test_handle_get(self):
        yield self.create_metric('foo', json.dumps('bar'))
        reply = yield self.dispatch_command('get', key='foo')
        self.check_reply(reply, success=True, value='bar')

    @inlineCallbacks
    def test_handle_get_for_unknown_key(self):
        reply = yield self.dispatch_command('get', key='foo')
        self.check_reply(reply, success=True, value=None)

    @inlineCallbacks
    def test_handle_delete(self):
        self.create_metric('foo', json.dumps('bar'))
        yield self.r_server.set('count#test_id', '1')
        reply = yield self.dispatch_command('delete', key='foo')
        self.check_reply(reply, success=True, existed=True)
        yield self.check_metric('foo', None, 0)

    @inlineCallbacks
    def test_handle_incr_default_amount(self):
        reply = yield self.dispatch_command('incr', key='foo')
        self.check_reply(reply, success=True, value=1)
        yield self.check_metric('foo', '1', 1)

    @inlineCallbacks
    def test_handle_incr_create(self):
        reply = yield self.dispatch_command('incr', key='foo', amount=2)
        self.check_reply(reply, success=True, value=2)
        yield self.check_metric('foo', '2', 1)

    @inlineCallbacks
    def test_handle_incr_existing(self):
        self.create_metric('foo', '2')
        reply = yield self.dispatch_command('incr', key='foo', amount=2)
        self.check_reply(reply, success=True, value=4)
        yield self.check_metric('foo', '4', 1)

    @inlineCallbacks
    def test_handle_incr_existing_non_int(self):
        self.create_metric('foo', 'a')
        reply = yield self.dispatch_command('incr', key='foo', amount=2)
        self.check_reply(reply, success=False)
        self.assertTrue(reply['reason'])
        yield self.check_metric('foo', 'a', 1)

    @inlineCallbacks
    def test_handle_incr_soft_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=80)
        reply = yield self.dispatch_command('incr', key='bar', amount=2)
        self.check_reply(reply, success=True)
        [limit_warning] = self.api.logs
        level, message = limit_warning
        self.assertEqual(level, logging.WARNING)
        self.assertEqual(
            message,
            'Redis soft limit of 80 keys reached for sandbox test_id. '
            'Once the hard limit of 100 is reached no more keys can '
            'be written.')

    @inlineCallbacks
    def test_handle_incr_hard_limit_reached(self):
        yield self.create_metric('foo', 'a', total_count=100)
        reply = yield self.dispatch_command('incr', key='bar', amount=2)
        self.check_reply(reply, success=False, reason='Too many keys')
        yield self.check_metric('bar', None, 100)
        [limit_error] = self.api.logs
        level, message = limit_error
        self.assertEqual(level, logging.ERROR)
        self.assertEqual(
            message,
            'Redis hard limit of 100 keys reached for sandbox test_id. '
            'No more keys can be written.')
