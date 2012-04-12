# -*- test-case-name: vumi.tests.test_start_worker -*-

import sys
import warnings

import yaml
from twisted.python import log, usage
from twisted.application.service import Service

from vumi.service import WorkerCreator
from vumi.utils import load_class_by_string
from vumi.errors import VumiError


def overlay_configs(*configs):
    """Non-recursively overlay a set of configuration dictionaries"""
    config = {}

    for overlay in configs:
        config.update(overlay)

    return config


def filter_null_values(config):
    """Remove keys with None values from a dictionary."""
    return dict(item for item in config.iteritems() if item[1] is not None)


def read_yaml_config(config_file, optional=True):
    """Parse an (usually) optional YAML config file."""
    if optional and config_file is None:
        return {}
    with file(config_file, 'r') as stream:
        # Assume we get a dict out of this.
        return yaml.safe_load(stream)


class VumiOptions(usage.Options):
    """
    Options global to everything vumi.
    """
    optParameters = [
        ["hostname", None, None, "AMQP broker"],
        ["port", None, None, "AMQP port", int],
        ["username", None, None, "AMQP username"],
        ["password", None, None, "AMQP password"],
        ["vhost", None, None, "AMQP virtual host"],
        ["specfile", None, None, "AMQP spec file"],
        ["vumi-config", None, None, "vumi config file"],
    ]

    default_vumi_options = {
        "hostname": "127.0.0.1",
        "port": 5672,
        "username": "vumi",
        "password": "vumi",
        "vhost": "/develop",
        "specfile": "amqp-spec-0-8.xml",
        }

    def get_vumi_options(self):
        # We don't want these to get in the way later.
        vumi_option_params = {}
        for opt in (i[0] for i in VumiOptions.optParameters):
            vumi_option_params[opt] = self.pop(opt)

        config_file = vumi_option_params.pop('vumi-config')

        return overlay_configs(
            self.default_vumi_options,
            read_yaml_config(config_file),
            filter_null_values(vumi_option_params))

    def postOptions(self):
        self.vumi_options = self.get_vumi_options()


class StartWorkerOptions(VumiOptions):
    """
    Options to the start_worker twistd plugin.
    """

    optFlags = [
        ["worker-help", None,
         "Print out a usage message for the worker-class and exit"],
        ]

    optParameters = [
        ["worker-class", None, None, "Class of a worker to start"],
        ["worker_class", None, None, "Deprecated. See --worker-class instead"],
        ["config", None, None, "YAML config file to load"],
    ]

    def __init__(self):
        VumiOptions.__init__(self)
        self.set_options = {}

    def opt_set_option(self, keyvalue):
        """Set a VUMI option (overrides config file values)."""
        key, _sep, value = keyvalue.partition(':')
        self.set_options[key] = value

    def exit(self):
        # So we can stub it out in tests.
        sys.exit(0)

    def do_worker_help(self):
        """Print out a usage message for the worker-class and exit"""
        worker_class = load_class_by_string(self.worker_class)
        print worker_class.__doc__
        self.exit()

    def get_worker_class(self):
        worker_class = self.opts.pop('worker-class')
        depr_worker_class = self.opts.pop('worker_class')

        if depr_worker_class is not None:
            warnings.warn("The --worker_class option is deprecated since"
                          " Vumi 0.3. Please use --worker-class instead.",
                          category=DeprecationWarning)
            if worker_class is None:
                worker_class = depr_worker_class

        if worker_class is None:
            raise VumiError("please specify --worker-class")

        return worker_class

    def get_worker_config(self):
        config_file = self.opts.pop('config')

        return overlay_configs(
            read_yaml_config(config_file),
            self.set_options)

    def postOptions(self):
        VumiOptions.postOptions(self)

        self.worker_class = self.get_worker_class()

        if self.opts.pop('worker-help'):
            self.do_worker_help()

        self.worker_config = self.get_worker_config()


class VumiService(Service):
    """Service started by the twistd plugin.

    This is a wrapper around a :class:`vumi.service.Worker` that handles
    configuration and service creation from the command line.
    """

    def __init__(self, options):
        self.options = options

    def startService(self):
        log.msg("Starting VumiService")

        worker_creator = WorkerCreator(self.options.vumi_options)
        self.worker = worker_creator.create_worker(
            self.options.worker_class, self.options.worker_config)
        return self.worker.startService()

    def stopService(self):
        log.msg("Stopping VumiService")
        return self.worker.stopService()
