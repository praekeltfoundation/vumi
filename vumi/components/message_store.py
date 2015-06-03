# -*- test-case-name: vumi.components.tests.test_message_store -*-
# -*- coding: utf-8 -*-

"""Message store."""

from calendar import timegm
from collections import defaultdict
from datetime import datetime
from uuid import uuid4
import itertools
import warnings

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import (
    TransportEvent, TransportUserMessage, parse_vumi_date, format_vumi_date)
from vumi.persist.model import Model, Manager
from vumi.persist.fields import (
    VumiMessage, ForeignKey, ManyToMany, ListOf, Tag, Dynamic, Unicode)
from vumi.persist.txriak_manager import TxRiakManager
from vumi import log
from vumi.components.message_store_cache import MessageStoreCache
from vumi.components.message_store_migrators import (
    EventMigrator, InboundMessageMigrator, OutboundMessageMigrator)


def to_reverse_timestamp(vumi_timestamp):
    """
    Turn a vumi_date-formatted string into a string that sorts in reverse order
    and can be turned back into a timestamp later.

    This is done by converting to a unix timestamp and subtracting it from
    0xffffffffff (2**40 - 1) to get a number well outside the range
    representable by the datetime module. The result is returned as a
    hexadecimal string.
    """
    timestamp = timegm(parse_vumi_date(vumi_timestamp).timetuple())
    return "%X" % (0xffffffffff - timestamp)


def from_reverse_timestamp(reverse_timestamp):
    """
    Turn a reverse timestamp string (from `to_reverse_timestamp()`) into a
    vumi_date-formatted string.
    """
    timestamp = 0xffffffffff - int(reverse_timestamp, 16)
    return format_vumi_date(datetime.utcfromtimestamp(timestamp))


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
    VERSION = 5
    MIGRATOR = OutboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    batches_with_addresses = ListOf(Unicode(), index=True)
    batches_with_addresses_reverse = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        self.batches_with_addresses = []
        self.batches_with_addresses_reverse = []
        timestamp = self.msg['timestamp']
        if not isinstance(timestamp, basestring):
            timestamp = format_vumi_date(timestamp)
        reverse_ts = to_reverse_timestamp(timestamp)
        for batch_id in self.batches.keys():
            self.batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['to_addr']))
            self.batches_with_addresses_reverse.append(
                u"%s$%s$%s" % (batch_id, reverse_ts, self.msg['to_addr']))
        return super(OutboundMessage, self).save()


class Event(Model):
    VERSION = 2
    MIGRATOR = EventMigrator

    # key is event_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    message_with_status = Unicode(index=True, null=True)
    batches_with_statuses_reverse = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        timestamp = self.event['timestamp']
        if not isinstance(timestamp, basestring):
            timestamp = format_vumi_date(timestamp)
        status = self.event['event_type']
        if status == "delivery_report":
            status = "%s.%s" % (status, self.event['delivery_status'])
        self.message_with_status = u"%s$%s$%s" % (
            self.message.key, timestamp, status)
        self.batches_with_statuses_reverse = []
        reverse_ts = to_reverse_timestamp(timestamp)
        for batch_id in self.batches.keys():
            self.batches_with_statuses_reverse.append(
                u"%s$%s$%s" % (batch_id, reverse_ts, status))
        return super(Event, self).save()


class InboundMessage(Model):
    VERSION = 5
    MIGRATOR = InboundMessageMigrator

    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batches = ManyToMany(Batch)

    # Extra fields for compound indexes
    batches_with_addresses = ListOf(Unicode(), index=True)
    batches_with_addresses_reverse = ListOf(Unicode(), index=True)

    def save(self):
        # We override this method to set our index fields before saving.
        self.batches_with_addresses = []
        self.batches_with_addresses_reverse = []
        timestamp = self.msg['timestamp']
        if not isinstance(timestamp, basestring):
            timestamp = format_vumi_date(timestamp)
        reverse_ts = to_reverse_timestamp(timestamp)
        for batch_id in self.batches.keys():
            self.batches_with_addresses.append(
                u"%s$%s$%s" % (batch_id, timestamp, self.msg['from_addr']))
            self.batches_with_addresses_reverse.append(
                u"%s$%s$%s" % (batch_id, reverse_ts, self.msg['from_addr']))
        return super(InboundMessage, self).save()


class ReconKeyManager(object):
    """
    A helper for tracking keys during cache recon.

    Keys are added one at a time from oldest to newest, and a buffer of recent
    keys is kept to allow old and new keys to be handled differently.
    """

    def __init__(self, start_timestamp, key_count):
        self.start_timestamp = start_timestamp
        self.key_count = key_count
        self.cache_keys = []
        self.new_keys = []

    def add_key(self, key, timestamp):
        """
        Add a key and timestamp to the manager.

        If ``timestamp`` is newer than :attr:`start_timestamp`, the pair is
        added to :attr:`new_keys`, otherwise it is added to :attr:`cache_keys`.
        If this causes :attr:`cache_keys` to grow larger than
        :attr:`key_count`, the earliest entry is removed and returned. If not,
        ``None`` is returned.

        It is assumed that keys will be added from oldest to newest.
        """
        if timestamp > self.start_timestamp:
            self.new_keys.append((key, timestamp))
            return None
        self.cache_keys.append((key, timestamp))
        if len(self.cache_keys) > self.key_count:
            return self.cache_keys.pop(0)
        return None

    def __iter__(self):
        return itertools.chain(self.cache_keys, self.new_keys)


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
    def reconcile_cache(self, batch_id, start_timestamp=None):
        """
        Rebuild the cache for the given batch.

        The ``start_timestamp`` parameter is used for testing only.
        """
        if start_timestamp is None:
            start_timestamp = format_vumi_date(datetime.utcnow())
        yield self.cache.clear_batch(batch_id)
        yield self.cache.batch_start(batch_id)
        yield self.reconcile_outbound_cache(batch_id, start_timestamp)
        yield self.reconcile_inbound_cache(batch_id, start_timestamp)

    @Manager.calls_manager
    def reconcile_inbound_cache(self, batch_id, start_timestamp):
        """
        Rebuild the inbound message cache.
        """
        key_manager = ReconKeyManager(
            start_timestamp, self.cache.TRUNCATE_MESSAGE_KEY_COUNT_AT)
        key_count = 0

        index_page = yield self.batch_inbound_keys_with_addresses(batch_id)
        while index_page is not None:
            for key, timestamp, addr in index_page:
                yield self.cache.add_from_addr(batch_id, addr)
                old_key = key_manager.add_key(key, timestamp)
                if old_key is not None:
                    key_count += 1
            index_page = yield index_page.next_page()

        yield self.cache.add_inbound_message_count(batch_id, key_count)
        for key, timestamp in key_manager:
            try:
                yield self.cache.add_inbound_message_key(
                    batch_id, key, self.cache.get_timestamp(timestamp))
            except:
                log.err()

    @Manager.calls_manager
    def reconcile_outbound_cache(self, batch_id, start_timestamp):
        """
        Rebuild the outbound message cache.
        """
        key_manager = ReconKeyManager(
            start_timestamp, self.cache.TRUNCATE_MESSAGE_KEY_COUNT_AT)
        key_count = 0
        status_counts = defaultdict(int)

        index_page = yield self.batch_outbound_keys_with_addresses(batch_id)
        while index_page is not None:
            for key, timestamp, addr in index_page:
                yield self.cache.add_to_addr(batch_id, addr)
                old_key = key_manager.add_key(key, timestamp)
                if old_key is not None:
                    key_count += 1
                    sc = yield self.get_event_counts(old_key[0])
                    for status, count in sc.iteritems():
                        status_counts[status] += count
            index_page = yield index_page.next_page()

        yield self.cache.add_outbound_message_count(batch_id, key_count)
        for status, count in status_counts.iteritems():
            yield self.cache.add_event_count(batch_id, status, count)
        for key, timestamp in key_manager:
            try:
                yield self.cache.add_outbound_message_key(
                    batch_id, key, self.cache.get_timestamp(timestamp))
                yield self.reconcile_event_cache(batch_id, key)
            except:
                log.err()

    @Manager.calls_manager
    def get_event_counts(self, message_id):
        """
        Get event counts for a particular message.

        This is used for old messages that we want to bulk-update.
        """
        status_counts = defaultdict(int)

        index_page = yield self.message_event_keys_with_statuses(message_id)
        while index_page is not None:
            for key, _timestamp, status in index_page:
                status_counts[status] += 1
                if status.startswith("delivery_report."):
                    status_counts["delivery_report"] += 1
            index_page = yield index_page.next_page()

        returnValue(status_counts)

    @Manager.calls_manager
    def reconcile_event_cache(self, batch_id, message_id):
        """
        Update the event cache for a particular message.
        """
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
    def _get_batches_from_outbound(self, msg_id):
        msg_record = yield self.outbound_messages.load(msg_id)
        if msg_record is not None:
            batch_ids = msg_record.batches.keys()
        else:
            batch_ids = []
        returnValue(batch_ids)

    @Manager.calls_manager
    def add_event(self, event, batch_ids=None):
        event_id = event['event_id']
        msg_id = event['user_message_id']
        event_record = yield self.events.load(event_id)
        if event_record is None:
            event_record = self.events(event_id, event=event, message=msg_id)
            if batch_ids is None:
                # If we aren't given batch_ids, get them from the outbound
                # message.
                batch_ids = yield self._get_batches_from_outbound(msg_id)
        else:
            event_record.event = event

        if batch_ids is not None:
            for batch_id in batch_ids:
                event_record.batches.add_key(batch_id)
                yield self.cache.add_event(batch_id, event)

        yield event_record.save()

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

    @Manager.calls_manager
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

    @Manager.calls_manager
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

    def _start_end_values(self, batch_id, start, end):
        if start is not None:
            start_value = "%s$%s" % (batch_id, start)
        else:
            start_value = "%s%s" % (batch_id, "#")  # chr(ord('$') - 1)
        if end is not None:
            # We append the "%" to this because we may have another field after
            # the timestamp and we want to include that in range.
            end_value = "%s$%s%s" % (batch_id, end, "%")  # chr(ord('$') + 1)
        else:
            end_value = "%s%s" % (batch_id, "%")  # chr(ord('$') + 1)
        return start_value, end_value

    @Manager.calls_manager
    def _query_batch_index(self, model_proxy, batch_id, index, max_results,
                           start, end, formatter):
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        start_value, end_value = self._start_end_values(batch_id, start, end)
        results = yield model_proxy.index_keys_page(
            index, start_value, end_value, max_results=max_results,
            return_terms=(formatter is not None))
        if formatter is not None:
            results = IndexPageWrapper(formatter, self, batch_id, results)
        returnValue(results)

    def batch_inbound_keys_with_timestamps(self, batch_id, max_results=None,
                                           start=None, end=None,
                                           with_timestamps=True):
        """
        Return all inbound message keys with (and ordered by) timestamps.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        :param bool with_timestamps:
            If set to ``False``, only the keys will be returned. The results
            will still be ordered by timestamp, however.

        This method performs a Riak index query.
        """
        formatter = key_with_ts_only_formatter if with_timestamps else None
        return self._query_batch_index(
            self.inbound_messages, batch_id, 'batches_with_addresses',
            max_results, start, end, formatter)

    def batch_outbound_keys_with_timestamps(self, batch_id, max_results=None,
                                            start=None, end=None,
                                            with_timestamps=True):
        """
        Return all outbound message keys with (and ordered by) timestamps.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        :param bool with_timestamps:
            If set to ``False``, only the keys will be returned. The results
            will still be ordered by timestamp, however.

        This method performs a Riak index query.
        """
        formatter = key_with_ts_only_formatter if with_timestamps else None
        return self._query_batch_index(
            self.outbound_messages, batch_id, 'batches_with_addresses',
            max_results, start, end, formatter)

    def batch_inbound_keys_with_addresses(self, batch_id, max_results=None,
                                          start=None, end=None):
        """
        Return all inbound message keys with (and ordered by) timestamps and
        addresses.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        This method performs a Riak index query.
        """
        return self._query_batch_index(
            self.inbound_messages, batch_id, 'batches_with_addresses',
            max_results, start, end, key_with_ts_and_value_formatter)

    def batch_outbound_keys_with_addresses(self, batch_id, max_results=None,
                                           start=None, end=None):
        """
        Return all outbound message keys with (and ordered by) timestamps and
        addresses.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        This method performs a Riak index query.
        """
        return self._query_batch_index(
            self.outbound_messages, batch_id, 'batches_with_addresses',
            max_results, start, end, key_with_ts_and_value_formatter)

    def batch_inbound_keys_with_addresses_reverse(self, batch_id,
                                                  max_results=None,
                                                  start=None, end=None):
        """
        Return all inbound message keys with timestamps and addresses.
        Results are ordered from newest to oldest.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        This method performs a Riak index query.
        """
        # We're using reverse timestamps, so swap start and end and convert to
        # reverse timestamps.
        if start is not None:
            start = to_reverse_timestamp(start)
        if end is not None:
            end = to_reverse_timestamp(end)
        start, end = end, start
        return self._query_batch_index(
            self.inbound_messages, batch_id, 'batches_with_addresses_reverse',
            max_results, start, end, key_with_rts_and_value_formatter)

    def batch_outbound_keys_with_addresses_reverse(self, batch_id,
                                                   max_results=None,
                                                   start=None, end=None):
        """
        Return all outbound message keys with timestamps and addresses.
        Results are ordered from newest to oldest.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        This method performs a Riak index query.
        """
        # We're using reverse timestamps, so swap start and end and convert to
        # reverse timestamps.
        if start is not None:
            start = to_reverse_timestamp(start)
        if end is not None:
            end = to_reverse_timestamp(end)
        start, end = end, start
        return self._query_batch_index(
            self.outbound_messages, batch_id, 'batches_with_addresses_reverse',
            max_results, start, end, key_with_rts_and_value_formatter)

    @Manager.calls_manager
    def message_event_keys_with_statuses(self, msg_id, max_results=None):
        """
        Return all event keys with (and ordered by) timestamps and statuses.

        :param str msg_id:
            The message_id to fetch event keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS

        This method performs a Riak index query. Unlike similar message key
        methods, start and end values are not supported as the number of events
        per message is expected to be small.
        """
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        start_value, end_value = self._start_end_values(msg_id, None, None)
        results = yield self.events.index_keys_page(
            'message_with_status', start_value, end_value,
            return_terms=True, max_results=max_results)
        returnValue(IndexPageWrapper(
            key_with_ts_and_value_formatter, self, msg_id, results))

    @Manager.calls_manager
    def batch_inbound_stats(self, batch_id, max_results=None,
                            start=None, end=None):
        """
        Return inbound message stats for the specified time range.

        Currently, message stats include total message count and unique address
        count.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS.

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        :returns:
            ``dict`` containing 'total' and 'unique_addresses' entries.

        This method performs multiple Riak index queries.
        """
        total = 0
        unique_addresses = set()

        start_value, end_value = self._start_end_values(batch_id, start, end)
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        raw_page = yield self.inbound_messages.index_keys_page(
            'batches_with_addresses', start_value, end_value,
            return_terms=True, max_results=max_results)
        page = IndexPageWrapper(
            key_with_ts_and_value_formatter, self, batch_id, raw_page)

        while page is not None:
            results = list(page)
            total += len(results)
            unique_addresses.update(addr for key, timestamp, addr in results)
            page = yield page.next_page()

        returnValue({
            "total": total,
            "unique_addresses": len(unique_addresses),
        })

    @Manager.calls_manager
    def batch_outbound_stats(self, batch_id, max_results=None,
                             start=None, end=None):
        """
        Return outbound message stats for the specified time range.

        Currently, message stats include total message count and unique address
        count.

        :param str batch_id:
            The batch_id to fetch keys for.

        :param int max_results:
            Number of results per page. Defaults to DEFAULT_MAX_RESULTS.

        :param str start:
            Optional start timestamp string matching VUMI_DATE_FORMAT.

        :param str end:
            Optional end timestamp string matching VUMI_DATE_FORMAT.

        :returns:
            ``dict`` containing 'total' and 'unique_addresses' entries.

        This method performs multiple Riak index queries.
        """
        total = 0
        unique_addresses = set()

        start_value, end_value = self._start_end_values(batch_id, start, end)
        if max_results is None:
            max_results = self.DEFAULT_MAX_RESULTS
        raw_page = yield self.outbound_messages.index_keys_page(
            'batches_with_addresses', start_value, end_value,
            return_terms=True, max_results=max_results)
        page = IndexPageWrapper(
            key_with_ts_and_value_formatter, self, batch_id, raw_page)

        while page is not None:
            results = list(page)
            total += len(results)
            unique_addresses.update(addr for key, timestamp, addr in results)
            page = yield page.next_page()

        returnValue({
            "total": total,
            "unique_addresses": len(unique_addresses),
        })


class IndexPageWrapper(object):
    """
    Index page wrapper that reformats index values into something easier to
    work with.

    This is a wrapper around the lower-level index page object from Riak and
    proxies a subset of its functionality.
    """
    def __init__(self, formatter, message_store, batch_id, index_page):
        self._formatter = formatter
        self._message_store = message_store
        self.manager = message_store.manager
        self._batch_id = batch_id
        self._index_page = index_page

    def _wrap_index_page(self, index_page):
        """
        Wrap a raw index page object if it is not None.
        """
        if index_page is not None:
            index_page = type(self)(
                self._formatter, self._message_store, self._batch_id,
                index_page)
        return index_page

    @Manager.calls_manager
    def next_page(self):
        """
        Fetch the next page of results.

        :returns:
            A new :class:`IndexPageWrapper` object containing the next page
            of results.
        """
        next_page = yield self._index_page.next_page()
        returnValue(self._wrap_index_page(next_page))

    def has_next_page(self):
        """
        Indicate whether there are more results to follow.

        :returns:
            ``True`` if there are more results, ``False`` if this is the last
            page.
        """
        return self._index_page.has_next_page()

    def __iter__(self):
        return (self._formatter(self._batch_id, r) for r in self._index_page)


def key_with_ts_and_value_formatter(batch_id, result):
    value, key = result
    prefix = batch_id + "$"
    if not value.startswith(prefix):
        raise ValueError(
            "Index value %r does not begin with expected prefix %r." % (
                value, prefix))
    suffix = value[len(prefix):]
    timestamp, delimiter, address = suffix.partition("$")
    if delimiter != "$":
        raise ValueError(
            "Index value %r does not match expected format." % (value,))
    return (key, timestamp, address)


def key_with_rts_and_value_formatter(batch_id, result):
    key, reverse_ts, value = key_with_ts_and_value_formatter(batch_id, result)
    return (key, from_reverse_timestamp(reverse_ts), value)


def key_with_ts_only_formatter(batch_id, result):
    key, timestamp, value = key_with_ts_and_value_formatter(batch_id, result)
    return (key, timestamp)


@inlineCallbacks
def add_batches_to_event(stored_event):
    """
    Post-migrate function to be used with `vumi_model_migrator` to add batches
    to stored events that don't have any.
    """
    if stored_event.batches.keys():
        # We already have batches, so there's no need to look them up.
        returnValue(False)

    outbound_messages = stored_event.manager.proxy(OutboundMessage)
    msg_record = yield outbound_messages.load(stored_event.message.key)
    if msg_record is not None:
        for batch_id in msg_record.batches.keys():
            stored_event.batches.add_key(batch_id)

    returnValue(True)
