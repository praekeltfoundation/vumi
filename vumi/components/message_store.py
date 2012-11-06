# -*- test-case-name: vumi.components.tests.test_message_store -*-
# -*- coding: utf-8 -*-

"""Message store."""

from uuid import uuid4

from twisted.internet.defer import returnValue

from vumi.message import TransportEvent, TransportUserMessage
from vumi.persist.model import Model, Manager
from vumi.persist.fields import (VumiMessage, ForeignKey, ListOf, Tag, Dynamic,
                                 Unicode)
from vumi import log
from vumi.components.message_store_cache import MessageStoreCache


class Batch(Model):
    # key is batch_id
    tags = ListOf(Tag())
    metadata = Dynamic(Unicode())


class CurrentTag(Model):
    # key is flattened tag
    current_batch = ForeignKey(Batch, null=True)
    tag = Tag()

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
    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(Batch, null=True)


class Event(Model):
    # key is message_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessage)


class InboundMessage(Model):
    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(Batch, null=True)


class MessageStore(object):
    """Vumi message store.

    Message batches, inbound messages, outbound messages, events and
    information about which batch a tag is currently associated with is
    stored in Riak.

    A small amount of information about the state of a batch (i.e. number
    of messages in the batch, messages sent, acknowledgements and delivery
    reports received) is stored in Redis.
    """

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
        yield self.reconcile_inbound_cache(batch_id)
        yield self.reconcile_outbound_cache(batch_id)

    @Manager.calls_manager
    def reconcile_inbound_cache(self, batch_id):
        # FIXME: We're loading messages one at a time here, which is stupid.
        inbound_keys = yield self.batch_inbound_keys(batch_id)
        for key in inbound_keys:
            try:
                msg = yield self.get_inbound_message(key)
                yield self.cache.add_inbound_message(batch_id, msg)
            except Exception:
                log.err('Unable to load inbound msg %s during recon of %s' % (
                    key, batch_id))

    @Manager.calls_manager
    def reconcile_outbound_cache(self, batch_id):
        # FIXME: We're loading messages one at a time here, which is stupid.
        outbound_keys = yield self.batch_outbound_keys(batch_id)
        for key in outbound_keys:
            try:
                msg = yield self.get_outbound_message(key)
                yield self.cache.add_outbound_message(batch_id, msg)
            except Exception:
                log.err('Unable to load outbound msg %s during recon of %s' % (
                    key, batch_id))

    @Manager.calls_manager
    def reconcile_event_cache(self, batch_id, message_id):
        events = yield self.message_events(message_id)
        for event in events:
            yield self.cache.add_event(batch_id, event)

    @Manager.calls_manager
    def batch_start(self, tags, **metadata):
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
        for tags_batch in self.manager.load_all_batches(CurrentTag, tag_keys):
            for tag in (yield tags_batch):
                tag.current_batch.set(None)
                yield tag.save()

    @Manager.calls_manager
    def add_outbound_message(self, msg, tag=None, batch_id=None):
        msg_id = msg['message_id']
        msg_record = self.outbound_messages(msg_id, msg=msg)

        if batch_id is None and tag is not None:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is not None:
                batch_id = tag_record.current_batch.key

        if batch_id is not None:
            msg_record.batch.key = batch_id
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
        event_record = self.events(event_id, event=event, message=msg_id)
        yield event_record.save()

        msg_record = yield self.outbound_messages.load(msg_id)
        if msg_record is not None:
            batch_id = msg_record.batch.key
            if batch_id is not None:
                yield self.cache.add_event(batch_id, event)

    @Manager.calls_manager
    def get_event(self, event_id):
        event = yield self.events.load(event_id)
        returnValue(event.event if event is not None else None)

    @Manager.calls_manager
    def add_inbound_message(self, msg, tag=None, batch_id=None):
        msg_id = msg['message_id']
        msg_record = self.inbound_messages(msg_id, msg=msg)

        if batch_id is None and tag is not None:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is not None:
                batch_id = tag_record.current_batch.key

        if batch_id is not None:
            msg_record.batch.key = batch_id
            self.cache.add_inbound_message(batch_id, msg)

        yield msg_record.save()

    @Manager.calls_manager
    def get_inbound_message(self, msg_id):
        msg = yield self.inbound_messages.load(msg_id)
        returnValue(msg.msg if msg is not None else None)

    def get_batch(self, batch_id):
        return self.batches.load(batch_id)

    def get_tag_info(self, tag):
        tagmdl = self.current_tags.load(tag)
        if tagmdl is None:
            tagmdl = self.current_tags(tag)
        return tagmdl

    def batch_status(self, batch_id):
        return self.cache.get_event_status(batch_id)

    def batch_outbound_keys(self, batch_id):
        mr = self.manager.mr_from_field(OutboundMessage, 'batch', batch_id)
        return mr.get_keys()

    def batch_inbound_keys(self, batch_id):
        mr = self.manager.mr_from_field(InboundMessage, 'batch', batch_id)
        return mr.get_keys()

    def message_event_keys(self, msg_id):
        mr = self.manager.mr_from_field(Event, 'message', msg_id)
        return mr.get_keys()

    def batch_inbound_count(self, batch_id):
        return self.inbound_messages.index_lookup(
            'batch', batch_id).get_count()

    def batch_outbound_count(self, batch_id):
        return self.outbound_messages.index_lookup(
            'batch', batch_id).get_count()
