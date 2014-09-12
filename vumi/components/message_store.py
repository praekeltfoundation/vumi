# -*- test-case-name: vumi.components.tests.test_message_store -*-
# -*- coding: utf-8 -*-

"""Message store."""

import warnings

from uuid import uuid4

from twisted.internet.defer import returnValue, inlineCallbacks

from vumi.message import TransportEvent, TransportUserMessage
from vumi.persist.model import Model, Manager
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ManyToMany, ListOf, Tag, Dynamic, Unicode)
from vumi.persist.txriak_manager import TxRiakManager
from vumi import log
from vumi.components.message_store_cache import MessageStoreCache
from vumi.components.message_store_migrators import (
    InboundMessageMigrator, OutboundMessageMigrator)


class Batch(Model):
    # key is batch_id
    tags = ListOf(Tag())
    metadata = Dynamic(Unicode())


class CurrentTag(Model):
    # key is flattened tag
    current_batch = ForeignKey(Batch, null=True)
    tag = Tag()
    metadata = Dynamic(Unicode())

    @staticmethod
    def _flatten_tag(tag):
        return "%s:%s" % tag

    @staticmethod
    def _split_key(key):
        return tuple(key.split(':', 1))

    @classmethod
    def _tag_and_key(cls, tag_or_key):
        if isinstance(tag_or_key, tuple):
            # key looks like a tag
            tag, key = tag_or_key, cls._flatten_tag(tag_or_key)
        else:
            tag, key = cls._split_key(tag_or_key), tag_or_key
        return tag, key

    def __init__(self, manager, key, _riak_object=None, **kw):
        tag, key = self._tag_and_key(key)
        if _riak_object is None:
            kw['tag'] = tag
        super(CurrentTag, self).__init__(manager, key,
                                         _riak_object=_riak_object, **kw)

    @classmethod
    def load(cls, manager, key, result=None):
        _tag, key = cls._tag_and_key(key)
        return super(CurrentTag, cls).load(manager, key, result)


class OutboundMessage(Model):

    VERSION = 1
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)


class Event(Model):
    # key is message_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessage)


class InboundMessage(Model):

    VERSION = 1
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)


class MessageStore(object):
    """Vumi message store.

    Message batches, inbound messages, outbound messages, events and
    information about which batch a tag is currently associated with is
    stored in Riak.

    A small amount of information about the state of a batch (i.e. number
    of messages in the batch, messages sent, acknowledgements and delivery
    reports received) is stored in Redis.
    """

    # The Python Riak client defaults to max_results=1000 in places.
    DEFAULT_MAX_RESULTS = 1000

    def __init__(self, manager, redis):
        self.manager = manager
        self.batches = manager.proxy(Batch)
        self.outbound_messages = manager.proxy(OutboundMessage)
        self.events = manager.proxy(Event)
        self.inbound_messages = manager.proxy(InboundMessage)
        self.current_tags = manager.proxy(CurrentTag)
        self.cache = MessageStoreCache(redis)

    @Manager.calls_manager
    def needs_reconciliation(self, batch_id, delta=0.01):
        """
        Check if a batch_id's cache values need to be reconciled with
        what's stored in the MessageStore.

        :param float delta:
            What an acceptable delta is for the cached values. Defaults to 0.01
            If the cached values are off by the delta then this returns True.
        """
        inbound = float((yield self.batch_inbound_count(batch_id)))
        cached_inbound = yield self.cache.count_inbound_message_keys(
            batch_id)

        if inbound and (abs(cached_inbound - inbound) / inbound) > delta:
            returnValue(True)

        outbound = float((yield self.batch_outbound_count(batch_id)))
        cached_outbound = yield self.cache.count_outbound_message_keys(
            batch_id)

        if outbound and (abs(cached_outbound - outbound) / outbound) > delta:
            returnValue(True)

        returnValue(False)

    @Manager.calls_manager
    def reconcile_cache(self, batch_id):
        yield self.cache.clear_batch(batch_id)
        yield self.cache.batch_start(batch_id)
        yield self.reconcile_outbound_cache(batch_id)
        yield self.reconcile_inbound_cache(batch_id)

    @Manager.calls_manager
    def reconcile_inbound_cache(self, batch_id):
        inbound_keys = yield self.batch_inbound_keys(batch_id)
        for key in inbound_keys:
            try:
                msg = yield self.get_inbound_message(key)
                yield self.cache.add_inbound_message(batch_id, msg)
            except Exception:
                log.err()

    @Manager.calls_manager
    def reconcile_outbound_cache(self, batch_id):
        outbound_keys = yield self.batch_outbound_keys(batch_id)
        for key in outbound_keys:
            try:
                msg = yield self.get_outbound_message(key)
                yield self.cache.add_outbound_message(batch_id, msg)
                yield self.reconcile_event_cache(batch_id, key)
            except Exception:
                log.err()

    @Manager.calls_manager
    def reconcile_event_cache(self, batch_id, message_id):
        event_keys = yield self.message_event_keys(message_id)
        for event_key in event_keys:
            event = yield self.get_event(event_key)
            yield self.cache.add_event(batch_id, event)

    @Manager.calls_manager
    def batch_start(self, tags=(), **metadata):
        batch_id = uuid4().get_hex()
        batch = self.batches(batch_id)
        batch.tags.extend(tags)
        for key, value in metadata.iteritems():
            batch.metadata[key] = value
        yield batch.save()

        for tag in tags:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is None:
                tag_record = self.current_tags(tag)
            tag_record.current_batch.set(batch)
            yield tag_record.save()

        yield self.cache.batch_start(batch_id)
        returnValue(batch_id)

    @Manager.calls_manager
    def batch_done(self, batch_id):
        batch = yield self.batches.load(batch_id)
        tag_keys = yield batch.backlinks.currenttags()
        for tags_bunch in self.manager.load_all_bunches(CurrentTag, tag_keys):
            for tag in (yield tags_bunch):
                tag.current_batch.set(None)
                yield tag.save()

    @Manager.calls_manager
    def add_outbound_message(self, msg, tag=None, batch_id=None, batch_ids=()):
        msg_id = msg['message_id']
        msg_record = yield self.outbound_messages.load(msg_id)
        if msg_record is None:
            msg_record = self.outbound_messages(msg_id, msg=msg)
        else:
            msg_record.msg = msg

        if batch_id is None and tag is not None:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is not None:
                batch_id = tag_record.current_batch.key

        batch_ids = list(batch_ids)
        if batch_id is not None:
            batch_ids.append(batch_id)

        for batch_id in batch_ids:
            msg_record.batches.add_key(batch_id)
            yield self.cache.add_outbound_message(batch_id, msg)

        yield msg_record.save()

    @Manager.calls_manager
    def get_outbound_message(self, msg_id):
        msg = yield self.outbound_messages.load(msg_id)
        returnValue(msg.msg if msg is not None else None)

    @Manager.calls_manager
    def add_event(self, event):
        event_id = event['event_id']
        msg_id = event['user_message_id']
        event_record = yield self.events.load(event_id)
        if event_record is None:
            event_record = self.events(event_id, event=event, message=msg_id)
        else:
            event_record.event = event
        yield event_record.save()

        msg_record = yield self.outbound_messages.load(msg_id)
        if msg_record is not None:
            for batch_id in msg_record.batches.keys():
                yield self.cache.add_event(batch_id, event)

    @Manager.calls_manager
    def get_event(self, event_id):
        event = yield self.events.load(event_id)
        returnValue(event.event if event is not None else None)

    @Manager.calls_manager
    def get_events_for_message(self, message_id):
        events = []
        event_keys = yield self.message_event_keys(message_id)
        for event_id in event_keys:
            event = yield self.get_event(event_id)
            events.append(event)
        returnValue(events)

    @Manager.calls_manager
    def add_inbound_message(self, msg, tag=None, batch_id=None, batch_ids=()):
        msg_id = msg['message_id']
        msg_record = yield self.inbound_messages.load(msg_id)
        if msg_record is None:
            msg_record = self.inbound_messages(msg_id, msg=msg)
        else:
            msg_record.msg = msg

        if batch_id is None and tag is not None:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is not None:
                batch_id = tag_record.current_batch.key

        batch_ids = list(batch_ids)
        if batch_id is not None:
            batch_ids.append(batch_id)

        for batch_id in batch_ids:
            msg_record.batches.add_key(batch_id)
            yield self.cache.add_inbound_message(batch_id, msg)

        yield msg_record.save()

    @Manager.calls_manager
    def get_inbound_message(self, msg_id):
        msg = yield self.inbound_messages.load(msg_id)
        returnValue(msg.msg if msg is not None else None)

    def get_batch(self, batch_id):
        return self.batches.load(batch_id)

    @Manager.calls_manager
    def get_tag_info(self, tag):
        tagmdl = yield self.current_tags.load(tag)
        if tagmdl is None:
            tagmdl = yield self.current_tags(tag)
        returnValue(tagmdl)

    def batch_status(self, batch_id):
        return self.cache.get_event_status(batch_id)

    def batch_outbound_keys(self, batch_id):
        return self.outbound_messages.index_keys('batches', batch_id)

    def batch_outbound_keys_page(self, batch_id, max_results=None,
                                 continuation=None):
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        return self.outbound_messages.index_keys_page(
            'batches', batch_id, max_results=max_results,
            continuation=continuation)

    def batch_outbound_keys_matching(self, batch_id, query):
        mr = self.outbound_messages.index_match(query, 'batches', batch_id)
        return mr.get_keys()

    def batch_inbound_keys(self, batch_id):
        return self.inbound_messages.index_keys('batches', batch_id)

    def batch_inbound_keys_page(self, batch_id, max_results=None,
                                continuation=None):
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        return self.inbound_messages.index_keys_page(
            'batches', batch_id, max_results=max_results,
            continuation=continuation)

    def batch_inbound_keys_matching(self, batch_id, query):
        mr = self.inbound_messages.index_match(query, 'batches', batch_id)
        return mr.get_keys()

    def message_event_keys(self, msg_id):
        return self.events.index_keys('message', msg_id)

    @Manager.calls_manager
    def batch_inbound_count(self, batch_id):
        keys = yield self.batch_inbound_keys(batch_id)
        returnValue(len(keys))

    @Manager.calls_manager
    def batch_outbound_count(self, batch_id):
        keys = yield self.batch_outbound_keys(batch_id)
        returnValue(len(keys))

    @inlineCallbacks
    def find_inbound_keys_matching(self, batch_id, query, ttl=None,
                                   wait=False):
        """
        Has the message search issue a `batch_inbound_keys_matching()`
        query and stores the resulting keys in the cache ordered by
        descending timestamp.

        :param str batch_id:
            The batch to search across
        :param list query:
            The list of dictionaries with query information.
        :param int ttl:
            How long to store the results for.
        :param bool wait:
            Only return the token after the matching, storing & ordering
            of keys has completed. Useful for testing.

        Returns a token with which the results can be fetched.

        NOTE:   This function can only be called from inside Twisted as
                it assumes that the result of `batch_inbound_keys_matching`
                is a Deferred.
        """
        assert isinstance(self.manager, TxRiakManager), (
            "manager is not an instance of TxRiakManager")
        token = yield self.cache.start_query(batch_id, 'inbound', query)
        deferred = self.batch_inbound_keys_matching(batch_id, query)
        deferred.addCallback(
            lambda keys: self.cache.store_query_results(batch_id, token, keys,
                                                        'inbound', ttl))
        if wait:
            yield deferred
        returnValue(token)

    @inlineCallbacks
    def find_outbound_keys_matching(self, batch_id, query, ttl=None,
                                    wait=False):
        """
        Has the message search issue a `batch_outbound_keys_matching()`
        query and stores the resulting keys in the cache ordered by
        descending timestamp.

        :param str batch_id:
            The batch to search across
        :param list query:
            The list of dictionaries with query information.
        :param int ttl:
            How long to store the results for.
        :param bool wait:
            Only return the token after the matching, storing & ordering
            of keys has completed. Useful for testing.

        Returns a token with which the results can be fetched.

        NOTE:   This function can only be called from inside Twisted as
                it depends on Deferreds being fired that aren't returned
                by the function itself.
        """
        token = yield self.cache.start_query(batch_id, 'outbound', query)
        deferred = self.batch_outbound_keys_matching(batch_id, query)
        deferred.addCallback(
            lambda keys: self.cache.store_query_results(batch_id, token, keys,
                                                        'outbound', ttl))
        if wait:
            yield deferred
        returnValue(token)

    def get_keys_for_token(self, batch_id, token, start=0, stop=-1, asc=False):
        """
        Returns the resulting keys of a search.

        :param str token:
            The token returned by `find_inbound_keys_matching()`
        """
        return self.cache.get_query_results(batch_id, token, start, stop, asc)

    def count_keys_for_token(self, batch_id, token):
        """
        Count the number of keys in the token's result set.
        """
        return self.cache.count_query_results(batch_id, token)

    def is_query_in_progress(self, batch_id, token):
        """
        Return True or False depending on whether or not the query is
        still running
        """
        return self.cache.is_query_in_progress(batch_id, token)

    def get_inbound_message_keys(self, batch_id, start=0, stop=-1,
                                 with_timestamp=False):
        warnings.warn("get_inbound_message_keys() is deprecated. Use "
                      "get_cached_inbound_message_keys().",
                      category=DeprecationWarning)
        return self.get_cached_inbound_message_keys(batch_id, start, stop,
                                                    with_timestamp)

    def get_cached_inbound_message_keys(self, batch_id, start=0, stop=-1,
                                        with_timestamp=False):
        """
        Return the keys ordered by descending timestamp.

        :param str batch_id:
            The batch_id to fetch keys for
        :param int start:
            Where to start from, defaults to 0 which is the first key.
        :param int stop:
            How many to fetch, defaults to -1 which is the last key.
        :param bool with_timestamp:
            Whether or not to return a list of (key, timestamp) tuples
            instead of only the list of keys.
        """
        return self.cache.get_inbound_message_keys(
            batch_id, start, stop, with_timestamp=with_timestamp)

    def get_outbound_message_keys(self, batch_id, start=0, stop=-1,
                                  with_timestamp=False):
        warnings.warn("get_outbound_message_keys() is deprecated. Use "
                      "get_cached_outbound_message_keys().",
                      category=DeprecationWarning)
        return self.get_cached_outbound_message_keys(batch_id, start, stop,
                                                     with_timestamp)

    def get_cached_outbound_message_keys(self, batch_id, start=0, stop=-1,
                                         with_timestamp=False):
        """
        Return the keys ordered by descending timestamp.

        :param str batch_id:
            The batch_id to fetch keys for
        :param int start:
            Where to start from, defaults to 0 which is the first key.
        :param int stop:
            How many to fetch, defaults to -1 which is the last key.
        :param bool with_timestamp:
            Whether or not to return a list of (key, timestamp) tuples
            instead of only the list of keys.
        """
        return self.cache.get_outbound_message_keys(
            batch_id, start, stop, with_timestamp=with_timestamp)
