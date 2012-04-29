"""Tests for vumi.scripts.vumi_tagpools."""

from pkg_resources import resource_filename

from twisted.trial.unittest import TestCase

from vumi.scripts.vumi_tagpools import ConfigHolder, Options


class TestConfigHolder(ConfigHolder):
    def __init__(self, *args, **kwargs):
        self.output = []
        super(TestConfigHolder, self).__init__(*args, **kwargs)

    def emit(self, s):
        self.output.append(s)


def run_cfg(args):
    args = ["--config",
            resource_filename(__name__, "sample-tagpool-cfg.yaml")] + args
    options = Options()
    options.parseOptions(args)
    cfg = TestConfigHolder(options)
    cfg.run()
    return cfg


class CreatePoolCmdTestCase(TestCase):
    def test_create_pool_range_tags(self):
        cfg = run_cfg(["create-pool", "shortcode"])
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
        cfg = run_cfg(["create-pool", "xmpp"])
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


class PurgePoolCmdTestCase(TestCase):
    # TODO: finish
    pass


class ListKeysCmdTestCase(TestCase):
    # TODO: finish
    pass


class ListPoolsCmdTestCase(TestCase):
    # TODO: finish
    pass
