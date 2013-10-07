# -*- test-case-name: vumi.scripts.tests.test_model_migrator -*-
import sys

from twisted.python import usage

from vumi.utils import load_class_by_string
from vumi.persist.riak_manager import RiakManager


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


class ProgressEmitter(object):
    """Report progress as a percentage to an emitter."""

    def __init__(self, total, emit):
        self.emit = emit
        self.total = total
        self.percentage = 0

    def _calculate_percentage(self, value):
        if value == 0:
            return 0
        return int(value * 100.0 / self.total)

    def update(self, value):
        old_percentage = self.percentage
        self.percentage = self._calculate_percentage(value)
        if self.percentage != old_percentage:
            self.emit(self.percentage)


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
        return RiakManager.from_config(riak_config)

    def emit(self, s):
        print s

    def run(self):
        dry_run = self.options["dry-run"]
        if self.options["keys"] is not None:
            keys = self.options["keys"].split(",")
            self.emit("Migrating %d specified keys ..." % len(keys))
        else:
            keys = self.model.all_keys()
            self.emit("%d keys found. Migrating ..." % len(keys))
        # Depending on our Riak client, Python version, and JSON library we may
        # get bytes or unicode here.
        keys = [k.decode('utf-8') if isinstance(k, str) else k for k in keys]
        progress = ProgressEmitter(
            len(keys),
            lambda p: self.emit("%s%% complete." % (p,))
        )
        for i, key in enumerate(keys):
            try:
                obj = self.model.load(key)
                if obj is not None:
                    if not dry_run:
                        obj.save()
                else:
                    self.emit("Skipping tombstone key %r." % (key,))
            except Exception, e:
                self.emit("Failed to migrate key %r:" % (key,))
                self.emit("  %s: %s" % (type(e).__name__, e))
            progress.update(i)
        self.emit("Done.")


if __name__ == '__main__':
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    cfg = ModelMigrator(options)
    cfg.run()
