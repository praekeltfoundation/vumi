# -*- test-case-name: vumi.scripts.tests.test_model_migrator -*-
import sys

from twisted.internet.defer import inlineCallbacks, gatherResults, succeed
from twisted.internet.task import react
from twisted.python import usage

from vumi.utils import load_class_by_string
from vumi.persist.txriak_manager import TxRiakManager


class Options(usage.Options):
    optParameters = [
        ["model", "m", None,
         "Full Python name of the model class to migrate."
         " E.g. 'vumi.components.message_store.InboundMessage'."],
        ["bucket-prefix", "b", None,
         "The bucket prefix for the Riak manager."],
        ["keys", None, None,
         "Migrate these specific keys rather than the whole bucket."
         " E.g. --keys 'foo,bar,baz'"],
        ["concurrent-migrations", None, "20",
         "The number of concurrent migrations to perform."],
        ["index-page-size", None, "1000",
         "The number of key to fetch in each index query."],
    ]

    optFlags = [
        ["dry-run", None, "Don't save anything back to Riak."],
    ]

    longdesc = """Offline model migrator. Necessary for updating
                  models when index names change so that old model
                  instances remain findable by index searches.
                  """

    def postOptions(self):
        if self['model'] is None:
            raise usage.UsageError("Please specify a model class.")
        if self['bucket-prefix'] is None:
            raise usage.UsageError("Please specify a bucket prefix.")
        self['concurrent-migrations'] = int(self['concurrent-migrations'])
        self['index-page-size'] = int(self['index-page-size'])


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


class FakeIndexPage(object):
    def __init__(self, keys, page_size):
        self._keys = keys
        self._page_size = page_size

    def __iter__(self):
        return iter(self._keys[:self._page_size])

    def has_next_page(self):
        return len(self._keys) > self._page_size

    def next_page(self):
        return succeed(
            type(self)(self._keys[self._page_size:], self._page_size))


class ModelMigrator(object):
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

    @inlineCallbacks
    def migrate_key(self, key, dry_run):
        try:
            obj = yield self.model.load(key)
            if obj is not None:
                if not dry_run:
                    yield obj.save()
            else:
                self.emit("Skipping tombstone key %r." % (key,))
        except Exception, e:
            self.emit("Failed to migrate key %r:" % (key,))
            self.emit("  %s: %s" % (type(e).__name__, e))

    def migrate_keys(self, _result, keys_list, dry_run):
        """
        Migrate keys from `keys_list` until there are none left.

        This method is expected to be called multiple times concurrently with
        all instances sharing the same `keys_list`.
        """
        if not keys_list:
            # Nothing left to migrate.
            return succeed(None)

        key = keys_list.pop(0)
        d = self.migrate_key(key, dry_run)
        d.addCallback(self.migrate_keys, keys_list, dry_run)
        return d

    def migrate_page(self, keys, dry_run):
        # Depending on our Riak client, Python version, and JSON library we may
        # get bytes or unicode here.
        keys = [k.decode('utf-8') if isinstance(k, str) else k for k in keys]
        return gatherResults([
            self.migrate_keys(None, keys, dry_run)
            for _ in xrange(self.options["concurrent-migrations"])])

    @inlineCallbacks
    def run(self):
        dry_run = self.options["dry-run"]
        if self.options["keys"] is not None:
            keys = self.options["keys"].split(",")
            self.emit("Migrating %d specified keys ..." % len(keys))
            emit_progress = lambda t: self.emit(
                "%s of %s objects migrated." % (t, len(keys)))
            index_page = FakeIndexPage(keys, self.options["index-page-size"])
        else:
            self.emit("Migrating ...")
            emit_progress = lambda t: self.emit(
                "%s object%s migrated." % (t, "" if t == 1 else "s"))
            index_page = yield self.model.all_keys_page(
                max_results=self.options["index-page-size"])

        progress = ProgressEmitter(
            emit_progress, self.options["index-page-size"])
        processed = 0
        while index_page is not None:
            if index_page.has_next_page():
                next_page_d = index_page.next_page()
            else:
                next_page_d = succeed(None)
            batch_keys = list(index_page)
            yield self.migrate_page(batch_keys, dry_run)
            processed += len(batch_keys)
            progress.update(processed)
            index_page = yield next_page_d
        self.emit("Done, %s objects migrated." % (processed,))


def main(_reactor, name, *args):
    try:
        options = Options()
        options.parseOptions(args)
    except usage.UsageError, errortext:
        print '%s: %s' % (name, errortext)
        print '%s: Try --help for usage details.' % (name,)
        sys.exit(1)

    model_migrator = ModelMigrator(options)
    return model_migrator.run()


if __name__ == '__main__':
    react(main, sys.argv)
