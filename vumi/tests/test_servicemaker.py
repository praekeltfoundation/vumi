import os

from vumi.servicemaker import (
    VumiOptions, StartWorkerOptions, VumiWorkerServiceMaker)
from vumi import servicemaker
from vumi.tests.helpers import VumiTestCase


class OptionsTestCase(VumiTestCase):
    "Base class for handling options files"

    def mk_config_file(self, name, lines=None):
        self.config_file[name] = self.mktemp()
        tempfile = open(self.config_file[name], 'w')
        if lines is not None:
            tempfile.write('\n'.join(lines))
        tempfile.close()

    def setUp(self):
        self.config_file = {}


class TestVumiOptions(OptionsTestCase):
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


class OldConfigWorker(object):
    """Dummy worker for testing --worker-help on old workers
    without CONFIG_CLASS"""


class DummyConfigClass(object):
    """Extra bit of doc string for testing --worker-help."""


class NewConfigWorker(object):
    """Dummy worker for testing --worker-help on new workers
    that support CONFIG_CLASS"""

    CONFIG_CLASS = DummyConfigClass


class TestStartWorkerOptions(OptionsTestCase):
    def test_override(self):
        options = StartWorkerOptions()
        options.parseOptions(['--worker-class', 'foo.FooWorker',
                              '--set-option', 'blah:bleh',
                              '--set-option', 'hungry:supper',
                              ])
        self.assertEqual(VumiOptions.default_vumi_options,
                         options.vumi_options)
        self.assertEqual({}, options.opts)
        self.assertEqual({
                'blah': 'bleh',
                'hungry': 'supper',
                }, options.worker_config)

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

    def test_config_file_with_include(self):
        # Create include file
        self.mk_config_file('provider_prefixes', [
            '"+27606": "vodacom"',
            '"+27603": "mtn"'])
        # Create root file
        self.mk_config_file('worker', [
            'transport_name: foo',
            'middleware:',
            '  - provider_inbound_mw: vumi.provider.foo',
            'provider_inbound_mw:'])
        # Find rel path between include and root files
        rel_path_providers = os.path.relpath(
            self.config_file['provider_prefixes'],
            os.path.dirname(self.config_file['worker']))
        # Add rel path to root file
        with open(self.config_file['worker'], 'a') as f:
            f.write('    !include %s\n' % rel_path_providers)
        # Process config files
        options = StartWorkerOptions()
        options.parseOptions([
            '--worker-class', 'foo.fooWorker',
            '--config', self.config_file['worker']])
        self.assertEqual(
            options.worker_config['provider_inbound_mw'],
            {
                '+27606': 'vodacom',
                '+27603': 'mtn',
            })

    def test_config_file_override(self):
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

    def check_worker_help(self, worker_class_name, expected_emits):
        exits, emits = [], []
        options = StartWorkerOptions()
        options.exit = lambda: exits.append(None)  # stub out exit
        options.emit = lambda text: emits.append(text)
        options.parseOptions(['--worker-class', worker_class_name,
                              '--worker-help',
                              ])
        self.assertEqual(len(exits), 1)
        self.assertEqual(emits, expected_emits)

    def test_old_style_config_worker_help(self):
        self.check_worker_help('vumi.tests.test_servicemaker.OldConfigWorker',
                               [OldConfigWorker.__doc__, ""])

    def test_new_style_config_worker_help(self):
        self.check_worker_help('vumi.tests.test_servicemaker.NewConfigWorker',
                               [NewConfigWorker.__doc__,
                                NewConfigWorker.CONFIG_CLASS.__doc__,
                                ""])


class DummyService(object):
    name = "Dummy"


class TestVumiWorkerServiceMaker(OptionsTestCase):

    def test_make_worker(self):
        self.mk_config_file('worker', ["transport_name: sphex"])
        options = StartWorkerOptions()
        options.parseOptions(['--worker-class', 'vumi.demos.words.EchoWorker',
                              '--config', self.config_file['worker'],
                              ])
        maker = VumiWorkerServiceMaker()
        worker = maker.makeService(options)
        self.assertEqual({'transport_name': 'sphex'}, worker.config)

    def test_make_worker_with_sentry(self):
        services = []
        dummy_service = DummyService()

        def service(*a, **kw):
            services.append((a, kw))
            return dummy_service

        self.patch(servicemaker, 'SentryLoggerService', service)
        self.mk_config_file('worker', ["transport_name: sphex"])
        options = StartWorkerOptions()
        options.parseOptions(['--worker-class', 'vumi.demos.words.EchoWorker',
                              '--config', self.config_file['worker'],
                              '--sentry', 'http://1:2@example.com/2/',
                              ])
        maker = VumiWorkerServiceMaker()
        worker = maker.makeService(options)
        self.assertEqual(services, [
                (('http://1:2@example.com/2/',
                  'echoworker',
                  'global:echoworker'), {})
        ])
        self.assertTrue(dummy_service in worker.services)

    def test_make_worker_with_threadpool_size(self):
        """
        The reactor threadpool can be resized with a command line option.
        """
        from twisted.internet import reactor

        old_maxthreads = reactor.getThreadPool().max
        self.add_cleanup(reactor.suggestThreadPoolSize, old_maxthreads)
        # Explicitly set the threadpool size to something different from the
        # value we're testing with.
        reactor.suggestThreadPoolSize(5)

        self.mk_config_file('worker', ["transport_name: sphex"])
        maker = VumiWorkerServiceMaker()

        # By default, we don't touch the threadpool.
        options = StartWorkerOptions()
        options.parseOptions([
            '--worker-class', 'vumi.demos.words.EchoWorker',
            '--config', self.config_file['worker'],
        ])
        worker = maker.makeService(options)
        self.assertEqual({'transport_name': 'sphex'}, worker.config)
        self.assertEqual(reactor.getThreadPool().max, 5)

        # If asked, we set the threadpool's maximum size.
        options_mt = StartWorkerOptions()
        options_mt.parseOptions([
            '--worker-class', 'vumi.demos.words.EchoWorker',
            '--config', self.config_file['worker'],
            '--maxthreads', '2',
        ])
        worker = maker.makeService(options_mt)
        self.assertEqual({'transport_name': 'sphex'}, worker.config)
        self.assertEqual(reactor.getThreadPool().max, 2)
