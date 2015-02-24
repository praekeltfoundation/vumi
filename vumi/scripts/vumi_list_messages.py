#!/usr/bin/env python
# -*- test-case-name: vumi.scripts.tests.test_vumi_list_messages -*-

import sys

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import react
from twisted.python import usage

from vumi.components.message_store import MessageStore
from vumi.persist.txriak_manager import TxRiakManager


class Options(usage.Options):
    optParameters = [
        ["batch", None, None,
         "Batch identifier to list messages for."],
        ["bucket-prefix", "b", None,
         "The bucket prefix for the Riak manager."],
        ["direction", None, None,
         "Message direction. Valid values are `inbound' and `outbound'."],
        ["index-page-size", None, "1000",
         "The number of keys to fetch in each index query."],
    ]

    longdesc = """
    Index-based message store lister. For each message, the timestamp, remote
    address, and message_id are returned in a comma-separated format.
    """

    def postOptions(self):
        if self["batch"] is None:
            raise usage.UsageError("Please specify a batch.")
        if self["direction"] not in ["inbound", "outbound"]:
            raise usage.UsageError("Please specify a valid direction.")
        if self["bucket-prefix"] is None:
            raise usage.UsageError("Please specify a bucket prefix.")
        self["index-page-size"] = int(self['index-page-size'])


class MessageLister(object):
    def __init__(self, options):
        self.options = options
        riak_config = {
            'bucket_prefix': options['bucket-prefix'],
        }
        self.manager = self.get_riak_manager(riak_config)
        self.mdb = MessageStore(self.manager, None)

    def get_riak_manager(self, riak_config):
        return TxRiakManager.from_config(riak_config)

    def emit(self, s):
        print s

    @inlineCallbacks
    def list_pages(self, index_page):
        while index_page is not None:
            next_page_d = index_page.next_page()
            for message_id, timestamp, addr in index_page:
                self.emit(",".join([timestamp, addr, message_id]))
            index_page = yield next_page_d

    @inlineCallbacks
    def run(self):
        index_func = {
            "inbound": self.mdb.batch_inbound_keys_with_addresses,
            "outbound": self.mdb.batch_outbound_keys_with_addresses,
        }[self.options["direction"]]
        index_page = yield index_func(
            self.options["batch"], max_results=self.options["index-page-size"])
        yield self.list_pages(index_page)


def main(_reactor, name, *args):
    try:
        options = Options()
        options.parseOptions(args)
    except usage.UsageError, errortext:
        print '%s: %s' % (name, errortext)
        print '%s: Try --help for usage details.' % (name,)
        sys.exit(1)

    model_counter = MessageLister(options)
    return model_counter.run()


if __name__ == '__main__':
    react(main, sys.argv)
