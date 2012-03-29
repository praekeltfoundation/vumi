# -*- test-case-name: vumi.application.tests.test_message_store -*-
# -*- coding: utf-8 -*-

"""Message store."""

from uuid import uuid4
from datetime import datetime

from vumi.message import (TransportEvent, TransportUserMessage,
                          from_json, to_json, VUMI_DATE_FORMAT)


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

    def __init__(self, r_server, r_prefix):
        self.r_server = r_server
        self.r_prefix = r_prefix

    def batch_start(self, tags):
        batch_id = uuid4().get_hex()
        batch_common = {u'tags': tags}
        tag_common = {u'current_batch_id': batch_id}
        self._init_status(batch_id)
        self._put_common('batches', batch_id, 'common', batch_common)
        self._put_row('batches', batch_id, 'messages', {})
        for tag in tags:
            self._put_common('tags', self._tag_key(tag), 'common', tag_common)
        return batch_id

    def batch_done(self, batch_id):
        tags = self.batch_common(batch_id)['tags']
        tag_common = {u'current_batch_id': None}
        if tags is not None:
            for tag in tags:
                self._put_common('tags', self._tag_key(tag), 'common',
                                 tag_common)

    def add_message(self, batch_id, msg):
        msg_id = msg['message_id']
        self._put_msg('messages', msg_id, 'body', msg)
        self._put_row('messages', msg_id, 'events', {})

        self._put_row('messages', msg_id, 'batches', {batch_id: '1'})
        self._put_row('batches', batch_id, 'messages', {msg_id: '1'})

        self._inc_status(batch_id, 'message')
        self._inc_status(batch_id, 'sent')

    def get_message(self, msg_id):
        return self._get_msg('messages', msg_id, 'body', TransportUserMessage)

    def add_event(self, event):
        event_id = event['event_id']
        self._put_msg('events', event_id, 'body', event)
        msg_id = event['user_message_id']
        self._put_row('messages', msg_id, 'events', {event_id: '1'})

        event_type = event['event_type']
        for batch_id in self._get_row('messages', msg_id, 'batches'):
            self._inc_status(batch_id, event_type)

    def get_event(self, event_id):
        return self._get_msg('events', event_id, 'body',
                             TransportEvent)

    def add_inbound_message(self, msg):
        msg_id = msg['message_id']
        self._put_msg('inbound_messages', msg_id, 'body', msg)
        tag = self._map_inbound_msg_to_tag(msg)
        if tag is not None:
            batch_id = self.tag_common(tag)['current_batch_id']
            if batch_id is not None:
                self._put_row('batches', batch_id, 'replies', {msg_id: '1'})

    def get_inbound_message(self, msg_id):
        return self._get_msg('inbound_messages', msg_id, 'body',
                             TransportUserMessage)

    def batch_common(self, batch_id):
        common = self._get_common('batches', batch_id, 'common')
        tags = common['tags']
        if tags is not None:
            common['tags'] = [tuple(x) for x in tags]
        return common

    def batch_status(self, batch_id):
        return self._get_status(batch_id)

    def tag_common(self, tag):
        common = self._get_common('tags', self._tag_key(tag), 'common')
        if not common:
            common = {u'current_batch_id': None}
        return common

    def batch_messages(self, batch_id):
        return self._get_row('batches', batch_id, 'messages').keys()

    def batch_replies(self, batch_id):
        return self._get_row('batches', batch_id, 'replies').keys()

    def message_batches(self, msg_id):
        return self._get_row('messages', msg_id, 'batches').keys()

    def message_events(self, msg_id):
        return self._get_row('messages', msg_id, 'events').keys()

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

    # tag <-> batch mappings are stored in Redis

    def _tag_key(self, tag):
        return "%s:%s" % tag

    def _map_inbound_msg_to_tag(self, msg):
        # usually this tag is set by tagging middleware attached to
        # the transport
        tag = msg.get('tag')
        if tag is None:
            return None
        return tuple(msg.get('tag'))

    # interface to redis -- intentionally made to look
    # like a limited subset of HBase.

    def _get_msg(self, table, row_id, family, cls):
        payload = self._get_common(table, row_id, family)
        # TODO: this is a hack needed because from_json(to_json(x)) != x
        #       if x is a datetime. Remove this once from_json and to_json
        #       are fixed.
        payload['timestamp'] = datetime.strptime(payload['timestamp'],
                                                 VUMI_DATE_FORMAT)
        return cls(**payload)

    def _put_msg(self, table, row_id, family, msg):
        return self._put_common(table, row_id, family, msg.payload)

    def _get_common(self, table, row_id, family):
        """Retrieve and decode a set of JSON-encoded values."""
        data = self._get_row(table, row_id, family)
        pydata = dict((k.decode('utf-8'), from_json(v))
                      for k, v in data.items())
        return pydata

    def _put_common(self, table, row_id, family, pydata):
        """JSON-encode and update a set of values."""
        data = dict((k.encode('utf-8'), to_json(v)) for k, v
                    in pydata.items())
        return self._put_row(table, row_id, family, data)

    def _get_row(self, table, row_id, family):
        """Retreive a set of column values from storage."""
        r_key = self._row_key(table, row_id, family)
        return self.r_server.hgetall(r_key)

    def _put_row(self, table, row_id, family, data):
        """Update a set of column values in storage."""
        r_key = self._row_key(table, row_id, family)
        if data:
            self.r_server.hmset(r_key, data)

    def _row_key(self, table, row_id, family):
        """Internal method for use by _get_row and _put_row."""
        return ":".join([self.r_prefix, table, family, row_id])
