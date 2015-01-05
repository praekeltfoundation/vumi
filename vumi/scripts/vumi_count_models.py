#!/usr/bin/env python
# -*- test-case-name: vumi.scripts.tests.test_vumi_count_models -*-

import re
import sys

from twisted.internet.defer import inlineCallbacks, succeed
from twisted.internet.task import react
from twisted.python import usage

from vumi.utils import load_class_by_string
from vumi.persist.txriak_manager import TxRiakManager


class Options(usage.Options):
    optParameters = [
        ["model", "m", None,
         "Full Python name of the model class to count."
         " E.g. 'vumi.components.message_store.InboundMessage'."],
        ["bucket-prefix", "b", None,
         "The bucket prefix for the Riak manager."],
        ["index-field", None, None,
         "Field with index to query. If omitted, all keys in the bucket will"
         " be counted and no `index-value-*' parameters are allowed."],
        ["index-value", None, None, "Exact match value or start of range."],
        ["index-value-end", None, None,
         "End of range. If ommitted, an exact match query will be used."],
        ["index-value-regex", None, None, "Regex to filter index values."],
        ["index-page-size", None, "1000",
         "The number of keys to fetch in each index query."],
    ]

    longdesc = """
    Index-based model counter. This makes paginated index queries, optionally
    filters the results by applying a regex to the index value, and returns a
    count of all matching models.
    """

    def ensure_dependent_option(self, needed, needs):
        """
        Raise UsageError if `needs` is provided without `needed`.
        """
        if self[needed] is None and self[needs] is not None:
            raise usage.UsageError("%s requires %s to be specified." % (
                needs, needed))

    def postOptions(self):
        if self["model"] is None:
            raise usage.UsageError("Please specify a model class.")
        if self["bucket-prefix"] is None:
            raise usage.UsageError("Please specify a bucket prefix.")
        self.ensure_dependent_option("index-field", "index-value")
        self.ensure_dependent_option("index-value", "index-field")
        self.ensure_dependent_option("index-value", "index-value-end")
        self.ensure_dependent_option("index-value-end", "index-value-regex")
        self["index-page-size"] = int(self['index-page-size'])


class ProgressEmitter(object):
    """Report progress as the number of items processed to an emitter."""

    def __init__(self, emit, batch_size):
        self.emit = emit
        self.batch_size = batch_size
        self.processed = 0

    def update(self, value):
        if (value / self.batch_size) > (self.processed / self.batch_size):
            self.emit(value)
        self.processed = value


class ModelCounter(object):
    def __init__(self, options):
        self.options = options
        model_cls = load_class_by_string(options['model'])
        riak_config = {
            'bucket_prefix': options['bucket-prefix'],
        }
        manager = self.get_riak_manager(riak_config)
        self.model = manager.proxy(model_cls)

    def get_riak_manager(self, riak_config):
        return TxRiakManager.from_config(riak_config)

    def emit(self, s):
        print s

    def count_keys(self, keys, filter_regex):
        """
        Count keys in an index page, filtering by regex if necessary.
        """
        if filter_regex is not None:
            keys = [(v, k) for v, k in keys if filter_regex.match(v)]
        return len(keys)

    @inlineCallbacks
    def count_pages(self, index_page, filter_regex):
        emit_progress = lambda t: self.emit(
            "%s object%s counted." % (t, "" if t == 1 else "s"))
        progress = ProgressEmitter(
            emit_progress, self.options["index-page-size"])
        counted = 0
        while index_page is not None:
            if index_page.has_next_page():
                next_page_d = index_page.next_page()
            else:
                next_page_d = succeed(None)
            counted += self.count_keys(list(index_page), filter_regex)
            progress.update(counted)
            index_page = yield next_page_d
        self.emit("Done, %s object%s found." % (
            counted, "" if counted == 1 else "s"))

    @inlineCallbacks
    def count_all_keys(self):
        """
        Perform an index query to get all keys and count them.
        """
        self.emit("Counting all keys ...")
        index_page = yield self.model.all_keys_page(
            max_results=self.options["index-page-size"])
        yield self.count_pages(index_page, filter_regex=None)

    @inlineCallbacks
    def count_index_keys(self):
        """
        Perform an index query to get all matching keys and count them.
        """
        filter_regex = self.options["index-value-regex"]
        if filter_regex is not None:
            filter_regex = re.compile(filter_regex)
        self.emit("Counting ...")
        index_page = yield self.model.index_keys_page(
            field_name=self.options["index-field"],
            value=self.options["index-value"],
            end_value=self.options["index-value-end"],
            max_results=self.options["index-page-size"],
            return_terms=True)
        yield self.count_pages(index_page, filter_regex=filter_regex)

    def run(self):
        if self.options["index-field"] is None:
            return self.count_all_keys()
        else:
            return self.count_index_keys()


def main(_reactor, name, *args):
    try:
        options = Options()
        options.parseOptions(args)
    except usage.UsageError, errortext:
        print '%s: %s' % (name, errortext)
        print '%s: Try --help for usage details.' % (name,)
        sys.exit(1)

    model_counter = ModelCounter(options)
    return model_counter.run()


if __name__ == '__main__':
    react(main, sys.argv)
