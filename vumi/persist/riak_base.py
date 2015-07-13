"""Basic tools for building a Riak manager."""

import json

from riak import RiakClient

from vumi.persist.model import VumiRiakError


def _to_unicode(text, encoding='utf-8'):
    # If we already have unicode or `None`, there's nothing to do.
    if isinstance(text, (unicode, type(None))):
        return text
    # If we have a tuple, we need to do our thing with every element in it.
    if isinstance(text, tuple):
        return tuple(_to_unicode(item, encoding) for item in text)
    # If we get here, then we should have a bytestring.
    return text.decode(encoding)


class VumiRiakClientBase(object):
    """
    Wrapper around a RiakClient to manage resources better.
    """

    def __init__(self, **client_args):
        self._closed = False
        self._raw_client = RiakClient(**client_args)
        # Some versions of the riak client library use simplejson by
        # preference, which breaks some of our unicode assumptions. This makes
        # sure we're using stdlib json which doesn't sometimes return
        # bytestrings instead of unicode.
        self._client.set_encoder('application/json', json.dumps)
        self._client.set_encoder('text/json', json.dumps)
        self._client.set_decoder('application/json', json.loads)
        self._client.set_decoder('text/json', json.loads)

    @property
    def protocol(self):
        return self._raw_client.protocol

    @property
    def _client(self):
        """
        Raise an exception if closed, otherwise return underlying client.
        """
        if self._closed:
            raise VumiRiakError("Can't use closed Riak client.")
        return self._raw_client

    def close(self):
        self._closed = True
        return self._raw_client.close()

    def bucket(self, bucket_name):
        return self._client.bucket(bucket_name)

    def put(self, *args, **kw):
        return self._client.put(*args, **kw)

    def get(self, *args, **kw):
        return self._client.get(*args, **kw)

    def delete(self, *args, **kw):
        return self._client.delete(*args, **kw)

    def mapred(self, *args, **kw):
        return self._client.mapred(*args, **kw)

    def _purge_all(self, bucket_prefix):
        """
        Purge all objects and buckets properties belonging to buckets with the
        given prefix.

        NOTE: This operation should *ONLY* be used in tests.
        """
        # We need to use a potentially closed client here, so we bypass the
        # check and reclose afterwards if necessary.
        buckets = self._raw_client.get_buckets()
        for bucket in buckets:
            if bucket.name.startswith(bucket_prefix):
                for key in bucket.get_keys():
                    obj = bucket.get(key)
                    obj.delete()
                bucket.clear_properties()
        if self._closed:
            self.close()


class VumiIndexPageBase(object):
    """
    Wrapper around a page of index query results.

    Iterating over this object will return the results for the current page.
    """

    def __init__(self, index_page):
        self._index_page = index_page

    def __iter__(self):
        if self._index_page.stream:
            raise NotImplementedError("Streaming is not currently supported.")
        return (_to_unicode(item) for item in self._index_page)

    def __len__(self):
        return len(self._index_page)

    def __eq__(self, other):
        return self._index_page.__eq__(other)

    def has_next_page(self):
        """
        Indicate whether there are more results to follow.

        :returns:
            ``True`` if there are more results, ``False`` if this is the last
            page.
        """
        return self._index_page.has_next_page()

    @property
    def continuation(self):
        return _to_unicode(self._index_page.continuation)

    # Methods that touch the network.

    def next_page(self):
        raise NotImplementedError("Subclasses must implement this.")


class VumiRiakBucketBase(object):
    """
    Wrapper around a RiakBucket to manage network access better.
    """

    def __init__(self, riak_bucket):
        self._riak_bucket = riak_bucket

    def get_name(self):
        return self._riak_bucket.name

    # Methods that touch the network.

    def get_index(self, index_name, start_value, end_value=None,
                  return_terms=None):
        raise NotImplementedError("Subclasses must implement this.")

    def get_index_page(self, index_name, start_value, end_value=None,
                       return_terms=None, max_results=None, continuation=None):
        raise NotImplementedError("Subclasses must implement this.")


class VumiRiakObjectBase(object):
    """
    Wrapper around a RiakObject to manage network access better.
    """

    def __init__(self, riak_obj):
        self._riak_obj = riak_obj

    @property
    def key(self):
        return self._riak_obj.key

    def get_key(self):
        return self.key

    def get_content_type(self):
        return self._riak_obj.content_type

    def set_content_type(self, content_type):
        self._riak_obj.content_type = content_type

    def get_data(self):
        return self._riak_obj.data

    def set_data(self, data):
        self._riak_obj.data = data

    def set_encoded_data(self, encoded_data):
        self._riak_obj.encoded_data = encoded_data

    def set_data_field(self, key, value):
        self._riak_obj.data[key] = value

    def delete_data_field(self, key):
        del self._riak_obj.data[key]

    def get_indexes(self):
        return self._riak_obj.indexes

    def set_indexes(self, indexes):
        self._riak_obj.indexes = indexes

    def add_index(self, index_name, index_value):
        self._riak_obj.add_index(index_name, index_value)

    def remove_index(self, index_name, index_value=None):
        self._riak_obj.remove_index(index_name, index_value)

    def get_user_metadata(self):
        return self._riak_obj.usermeta

    def set_user_metadata(self, usermeta):
        self._riak_obj.usermeta = usermeta

    def get_bucket(self):
        raise NotImplementedError("Subclasses must implement this.")

    # Methods that touch the network.

    def _call_and_wrap(self, func):
        """
        Call a function that touches the network and wrap the result in this
        class.
        """
        raise NotImplementedError("Subclasses must implement this.")

    def store(self):
        return self._call_and_wrap(self._riak_obj.store)

    def reload(self):
        return self._call_and_wrap(self._riak_obj.reload)

    def delete(self):
        return self._call_and_wrap(self._riak_obj.delete)
