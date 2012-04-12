from twisted.trial.unittest import TestCase

from vumi.start_worker import VumiOptions, StartWorkerOptions


class OptionsTestCase(TestCase):
    "Base class for handling options files"

    def mk_config_file(self, name, lines=None):
        self.config_file[name] = self.mktemp()
        tempfile = open(self.config_file[name], 'w')
        if lines is not None:
            tempfile.write('\n'.join(lines))
        tempfile.close()

    def setUp(self):
        self.config_file = {}


class VumiOptionsTestCase(OptionsTestCase):
    def test_defaults(self):
        options = VumiOptions()
        options.parseOptions([])
        self.assertEqual({}, options.opts)
        self.assertEqual(VumiOptions.default_vumi_options,
                         options.vumi_options)

    def test_override(self):
        options = VumiOptions()
        options.parseOptions(['--hostname', 'blah',
                              '--username', 'haxor'])
        self.assertEqual({}, options.opts)
        self.assertEqual(dict(VumiOptions.default_vumi_options,
                              username='haxor', hostname='blah'),
                         options.vumi_options)

    def test_config_file(self):
        options = VumiOptions()
        self.mk_config_file('vumi', ["username: foo", "password: bar"])
        options.parseOptions(['--vumi-config', self.config_file['vumi']])
        self.assertEqual({}, options.opts)
        self.assertEqual(dict(VumiOptions.default_vumi_options,
                              username='foo', password='bar'),
                         options.vumi_options)

    def test_config_file_override(self):
        self.mk_config_file('vumi', ["username: foo", "password: bar"])
        options = VumiOptions()
        options.parseOptions(['--vumi-config', self.config_file['vumi'],
                              '--hostname', 'blah',
                              '--username', 'haxor'])
        self.assertEqual({}, options.opts)
        self.assertEqual(dict(VumiOptions.default_vumi_options,
                              username='haxor', password='bar',
                              hostname='blah'),
                         options.vumi_options)


class StartWorkerOptionsTestCase(OptionsTestCase):
    def test_config_file(self):
        self.mk_config_file('worker',
                            ["transport_name: sphex", "blah: thingy"])
        options = StartWorkerOptions()
        options.parseOptions(['--worker-class', 'foo.FooWorker',
                              '--config', self.config_file['worker'],
                              ])
        self.assertEqual(VumiOptions.default_vumi_options,
                         options.vumi_options)
        self.assertEqual({}, options.opts)
        self.assertEqual({
                'transport_name': 'sphex',
                'blah': 'thingy',
                }, options.worker_config)

    def test_config_overrides(self):
        self.mk_config_file('worker',
                            ["transport_name: sphex", "blah: thingy"])
        options = StartWorkerOptions()
        options.parseOptions(['--worker-class', 'foo.FooWorker',
                              '--config', self.config_file['worker'],
                              '--set-option', 'blah:bleh',
                              '--set-option', 'hungry:supper',
                              ])
        self.assertEqual(VumiOptions.default_vumi_options,
                         options.vumi_options)
        self.assertEqual({}, options.opts)
        self.assertEqual({
                'transport_name': 'sphex',
                'blah': 'bleh',
                'hungry': 'supper',
                }, options.worker_config)

    def test_with_vumi_opts(self):
        self.mk_config_file('vumi', ["username: foo", "password: bar"])
        self.mk_config_file('worker', ["transport_name: sphex"])
        options = StartWorkerOptions()
        options.parseOptions(['--vumi-config', self.config_file['vumi'],
                              '--hostname', 'blah',
                              '--username', 'haxor',
                              '--worker-class', 'foo.FooWorker',
                              '--config', self.config_file['worker'],
                              ])
        self.assertEqual(dict(VumiOptions.default_vumi_options,
                              username='haxor', password='bar',
                              hostname='blah'),
                         options.vumi_options)
        self.assertEqual({}, options.opts)


# No VumiService tests, because we'd have to stub out a whole bunch of stuff.
# I deliberately made it very small, though.
