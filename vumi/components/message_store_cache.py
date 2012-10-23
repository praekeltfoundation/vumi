# -*- test-case-name: vumi.components.tests.test_message_store_cache -*-
# -*- coding: utf-8 -*-


class MessageStoreCache(object):
    """
    A helper class to provide a view on information in the message store
    that is difficult to query straight from riak.
    """

    def __init__(self, redis):
        self.redis = redis

    def reconcile(self, message_store, batch_id):
        """
        Reconcile the cache with the data in Riak. This is a heavy process.
        """
        pass

    def add_outbound_message(self, batch_id, msg):
        """
        Add an outbound message to the cache for the given batch_id
        """
        pass

    def add_outbound_message_key(self, batch_id, message_key, timestamp):
        pass

    def add_event(self, batch_id, event):
        """
        Add an event to the cache for the given batch_id
        """
        pass

    def add_inbound_message(self, batch_id, msg):
        """
        Add an inbound message to the cache for the given batch_id
        """
        pass

    def add_inbound_message_key(self, batch_id, message_key, timestamp):
        pass

    def get_to_addrs(self):
        """
        Return a set of unique to_addrs addressed in this batch ordered
        by the most recent timestamp.
        """
        pass

    def get_to_addrs_count(self):
        """
        Return count of the unique to_addrs in this batch.
        """
        pass

    def get_inbound_message_keys(self):
        """
        Return a list of keys ordered according to their timestamps
        """
        pass

    def get_outbound_message_keys(self):
        """
        Return a list of keys ordered according to their timestamps.
        """
        pass
