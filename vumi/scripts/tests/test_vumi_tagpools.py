"""Tests for vumi.scripts.vumi_tagpools."""

from pkg_resources import resource_filename

from twisted.trial.unittest import TestCase

from vumi.tests.utils import import_skip


def make_cfg(args):
    try:
        from vumi.scripts.vumi_tagpools import ConfigHolder, Options
    except ImportError, e:
        import_skip(e, 'redis')

    class TestConfigHolder(ConfigHolder):
        def __init__(self, *args, **kwargs):
            self.output = []
            super(TestConfigHolder, self).__init__(*args, **kwargs)

        def emit(self, s):
            self.output.append(s)

    args = ["--config",
            resource_filename(__name__, "sample-tagpool-cfg.yaml")] + args
    options = Options()
    options.parseOptions(args)
    return TestConfigHolder(options)


class CreatePoolCmdTestCase(TestCase):
    def test_create_pool_range_tags(self):
        cfg = make_cfg(["create-pool", "shortcode"])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Creating pool shortcode ...',
            '  Setting metadata ...',
            '  Declaring 1000 tag(s) ...',
            '  Done.',
            ])
        self.assertEqual(cfg.tagpool.get_metadata("shortcode"),
                         {'transport_type': 'sms'})
        self.assertEqual(sorted(cfg.tagpool.free_tags("shortcode")),
                         [("shortcode", str(d)) for d in range(10001, 11001)])
        self.assertEqual(cfg.tagpool.inuse_tags("shortcode"), [])

    def test_create_pool_explicit_tags(self):
        cfg = make_cfg(["create-pool", "xmpp"])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Creating pool xmpp ...',
            '  Setting metadata ...',
            '  Declaring 1 tag(s) ...',
            '  Done.',
            ])
        self.assertEqual(cfg.tagpool.get_metadata("xmpp"),
                         {'transport_type': 'xmpp'})
        self.assertEqual(sorted(cfg.tagpool.free_tags("xmpp")),
                         [("xmpp", "me@example.com")])
        self.assertEqual(cfg.tagpool.inuse_tags("xmpp"), [])


class UpdatePoolMetadataCmdTestCase(TestCase):
    def test_create_pool_range_tags(self):
        cfg = make_cfg(["update-pool-metadata", "shortcode"])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Updating metadata for pool shortcode ...',
            '  Done.',
            ])
        self.assertEqual(cfg.tagpool.get_metadata("shortcode"),
                         {'transport_type': 'sms'})


class PurgePoolCmdTestCase(TestCase):
    def test_purge_pool(self):
        cfg = make_cfg(["purge-pool", "foo"])
        cfg.tagpool.declare_tags([("foo", "tag1"), ("foo", "tag2")])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Purging pool foo ...',
            '  Done.',
            ])
        self.assertEqual(cfg.tagpool.free_tags("foo"), [])
        self.assertEqual(cfg.tagpool.inuse_tags("foo"), [])
        self.assertEqual(cfg.tagpool.get_metadata("foo"), {})


class ListKeysCmdTestCase(TestCase):
    def setUp(self):
        self.test_tags = [("foo", "tag%d" % i) for
                          i in [1, 2, 3, 5, 6, 7, 9]]

    def test_list_keys_all_free(self):
        cfg = make_cfg(["list-keys", "foo"])
        cfg.tagpool.declare_tags(self.test_tags)
        cfg.run()
        self.assertEqual(cfg.output, [
            'Listing tags for pool foo ...',
            'Free tags:',
            '   tag[1-3], tag[5-7], tag9',
            'Tags in use:',
            '   -- None --',
            ])

    def test_list_keys_all_in_use(self):
        cfg = make_cfg(["list-keys", "foo"])
        cfg.tagpool.declare_tags(self.test_tags)
        for tag in self.test_tags:
            cfg.tagpool.acquire_tag("foo")
        cfg.run()
        self.assertEqual(cfg.output, [
            'Listing tags for pool foo ...',
            'Free tags:',
            '   -- None --',
            'Tags in use:',
            '   tag[1-3], tag[5-7], tag9',
            ])


class ListPoolsCmdTestCase(TestCase):
    def test_list_pools_with_only_pools_in_config(self):
        cfg = make_cfg(["list-pools"])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Pools defined in cfg and tagpool:',
            '   -- None --',
            'Pools only in cfg:',
            '   longcode, shortcode, xmpp',
            'Pools only in tagpool:',
            '   -- None --',
            ])

    def test_list_pools_with_all_pools_in_tagpool(self):
        cfg = make_cfg(["list-pools"])
        cfg.tagpool.declare_tags([("xmpp", "tag"), ("longcode", "tag"),
                                  ("shortcode", "tag")])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Pools defined in cfg and tagpool:',
            '   longcode, shortcode, xmpp',
            'Pools only in cfg:',
            '   -- None --',
            'Pools only in tagpool:',
            '   -- None --',
            ])

    def test_list_pools_with_all_sorts_of_pools(self):
        cfg = make_cfg(["list-pools"])
        cfg.tagpool.declare_tags([("xmpp", "tag"), ("other", "tag")])
        cfg.run()
        self.assertEqual(cfg.output, [
            'Pools defined in cfg and tagpool:',
            '   xmpp',
            'Pools only in cfg:',
            '   longcode, shortcode',
            'Pools only in tagpool:',
            '   other',
            ])
