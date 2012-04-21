# -*- test-case-name: vumi.persist.tests.test_message_store -*-
# -*- coding: utf-8 -*-

"""Message store."""

from uuid import uuid4

from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.message import TransportEvent, TransportUserMessage
from vumi.persist.model import Model
from vumi.persist.fields import VumiMessage, ForeignKey, ListOf, Tag


class Batch(Model):
    # key is batch_id
    tags = ListOf(Tag())


class CurrentTag(Model):
    # key is flattened tag
    current_batch = ForeignKey(Batch)
    tag = Tag()

    @staticmethod
    def _flatten_tag(tag):
        return "%s:%s" % tag

    @staticmethod
    def _split_key(key):
        return tuple(key.split(':', 1))

    def __init__(self, manager, key, **field_values):
        if isinstance(key, tuple):
            # key looks like a tag
            tag, key = key, self._flatten_tag(key)
        else:
            tag, key = self._split_key(key), key
        super(CurrentTag, self).__init__(manager, key, tag=tag,
                                         **field_values)


class OutboundMessage(Model):
    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(Batch)


class Event(Model):
    # key is message_id
    event = VumiMessage(TransportEvent)
    message = ForeignKey(OutboundMessage)


class InboundMessage(Model):
    # key is message_id
    msg = VumiMessage(TransportUserMessage)
    batch = ForeignKey(Batch)


class MessageStore(object):
    """Vumi Go message store.

    HBase-like data schema:

      # [row_id] -> [family] -> [columns]

      batches:
        batch_id -> common -> ['tag']
                 -> messages -> column names are message ids
                 -> replies -> column names are inbound_message ids

      tags:
        tag -> common -> ['current_batch_id']

      messages:
        message_id -> body -> column names are message fields,
                              values are JSON encoded
                   -> events -> column names are event ids
                   -> batches -> column names are batch ids

      inbound_messages:
        message_id -> body -> column names are message fields,
                              values are JSON encoded

      events:
        event_id -> body -> column names are message fields,
                            values are JSON encoded


    Possible future schema tweaks for later:

    * third_party_ids table that maps third party message ids
      to vumi message ids (third_pary:third_party_id -> data
      -> message_id)
    * Consider making message_id "batch_id:current_message_id"
      (this makes retrieving batches of messages fast, it
       might be better to have "timestamp:current_message_id").
    """

    def __init__(self, manager, r_server, r_prefix):
        self.batches = manager.proxy(Batch)
        self.outbound_messages = manager.proxy(OutboundMessage)
        self.events = manager.proxy(Event)
        self.inbound_messages = manager.proxy(InboundMessage)
        self.current_tags = manager.proxy(CurrentTag)
        # for batch status cache
        self.r_server = r_server
        self.r_prefix = r_prefix

    @inlineCallbacks
    def batch_start(self, tags):
        batch_id = uuid4().get_hex()
        batch = self.batches(batch_id)
        batch.tags.extend(tags)
        yield batch.save()

        for tag in tags:
            tag_record = yield self.current_tags.load(tag)
            if tag_record is None:
                tag_record = self.current_tags(tag)
            tag_record.current_batch.set(batch)
            yield tag_record.save()

        self._init_status(batch_id)
        returnValue(batch_id)

    @inlineCallbacks
    def batch_done(self, batch_id):
        batch = yield self.batches.load(batch_id)
        tags = yield batch.backlinks.currenttags()
        for tag in tags:
            tag.current_batch.set(None)
            yield tag.save()

    @inlineCallbacks
    def add_outbound_message(self, msg, tag=None, batch_id=None):
        msg_id = msg['message_id']
        msg_record = self.outbound_messages(msg_id)
        msg_record.msg = msg
        yield msg_record.save()

        if batch_id is None and tag is not None:
            tag_record = yield self.current_tags.load(tag)
            batch_id = tag_record.current_batch.key

        if batch_id is not None:
            batch = yield self.batches.load(batch_id)
            msg_record.batch.set(batch)
            yield msg_record.save()

            self._inc_status(batch_id, 'message')
            self._inc_status(batch_id, 'sent')

    @inlineCallbacks
    def get_outbound_message(self, msg_id):
        msg = yield self.outbound_messages.load(msg_id)
        returnValue(msg.msg)

    @inlineCallbacks
    def add_event(self, event):
        event_id = event['event_id']
        event_record = self.events(event_id)
        event_record.event = event
        msg_id = event['user_message_id']
        msg_record = yield self.outbound_messages.load(msg_id)
        event_record.message.set(msg_record)
        yield event_record.save()

        batch_record = yield msg_record.batch.get()
        self._inc_status(batch_record.key, event['event_type'])

    @inlineCallbacks
    def get_event(self, event_id):
        event = yield self.events.load(event_id)
        returnValue(event.event)

    @inlineCallbacks
    def add_inbound_message(self, msg, tag=None, batch_id=None):
        msg_id = msg['message_id']
        msg_record = self.inbound_messages(msg_id)
        msg_record.msg = msg
        yield msg_record.save()

        if batch_id is None and tag is not None:
            tag_record = yield self.current_tags.load(tag)
            batch_id = tag_record.current_batch.key

        if batch_id is not None:
            batch = yield self.batches.load(batch_id)
            msg_record.batch.set(batch)
            yield msg_record.save()

    @inlineCallbacks
    def get_inbound_message(self, msg_id):
        msg = yield self.inbound_messages.load(msg_id)
        returnValue(msg.msg)

    def get_batch(self, batch_id):
        return self.batches.load(batch_id)

    def get_tag_info(self, tag):
        return self.current_tags.load(tag)

    def batch_status(self, batch_id):
        return self._get_status(batch_id)

    @inlineCallbacks
    def batch_messages(self, batch_id):
        batch = yield self.batches.load(batch_id)
        messages = yield batch.backlinks.outboundmessages()
        returnValue([m.msg for m in messages])

    @inlineCallbacks
    def batch_replies(self, batch_id):
        batch = yield self.batches.load(batch_id)
        messages = yield batch.backlinks.inboundmessages()
        returnValue([m.msg for m in messages])

    @inlineCallbacks
    def message_events(self, msg_id):
        message = yield self.outbound_messages.load(msg_id)
        events = yield message.backlinks.events()
        returnValue([e.event for e in events])

    # batch status is stored in Redis as a cache of batch progress

    def _batch_key(self, batch_id):
        return ":".join([self.r_prefix, "batches", "status", batch_id])

    def _init_status(self, batch_id):
        batch_key = self._batch_key(batch_id)
        events = TransportEvent.EVENT_TYPES.keys() + ['message', 'sent']
        initial_status = dict((event, '0') for event in events)
        self.r_server.hmset(batch_key, initial_status)

    def _inc_status(self, batch_id, event):
        batch_key = self._batch_key(batch_id)
        self.r_server.hincrby(batch_key, event, 1)

    def _get_status(self, batch_id):
        batch_key = self._batch_key(batch_id)
        raw_statuses = self.r_server.hgetall(batch_key)
        statuses = dict((k, int(v)) for k, v in raw_statuses.items())
        return statuses
