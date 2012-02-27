import sys
import yaml
from zope.interface import implements
from twisted.python import log
from twisted.application.service import IServiceMaker, Service
from twisted.plugin import IPlugin

from vumi.service import Options, WorkerCreator
from vumi.utils import load_class_by_string
from vumi.errors import VumiError


# This is the actual service that is started, this the thing that runs
# in the background and starts a worker.
class VumiService(Service):

    # it receives the dictionary of options from the command line
    def __init__(self, options):
        self.options = options

    # Twistd calls this method at boot
    def startService(self):
        log.msg("Starting VumiService")
        vumi_options = {}
        for opt in [i[0] for i in Options.optParameters]:
            vumi_options[opt] = self.options.pop(opt)

        worker_creator = WorkerCreator(vumi_options)

        # We need an initial worker. This would either be a normal
        # worker class (if we're using the old one-per-process model)
        # or a worker that loads new workers.
        worker_class = self.options.pop("worker-class")
        if worker_class is None:
            # fallback to deprecated --worker_class option
            worker_class = self.options.pop('worker_class')

        if not worker_class:
            raise VumiError("please specify --worker-class")

        config = {}
        # load the config file if specified
        config_file = self.options.pop('config')
        if config_file:
            with file(config_file, 'r') as stream:
                config.update(yaml.safe_load(stream))

        # add options set with --set-option
        config.update(self.options.set_options)

        self.worker = worker_creator.create_worker(worker_class, config)
        return self.worker.startService()

    # Twistd calls this method at shutdown
    def stopService(self):
        log.msg("Stopping VumiService")
        return self.worker.stopService()


# Extend the default Vumi options with whatever options your service needs
class BasicSet(Options):
    optFlags = [
        ["worker-help", None, "Print out a usage message for the worker_class"
                              " and exit"],
        ]
    optParameters = Options.optParameters + [
        ["worker-class", None, None, "Class of a worker to start"],
        ["worker_class", None, None, "Deprecated. See --worker-class instead"],
        ["config", None, None, "YAML config file to load"],
        ["set-option", None, None, "Override a config file option"],
    ]

    def opt_worker_help(self):
        worker_class_name = self.opts.pop('worker-class')
        if worker_class_name is None:
            # fallback to deprecated --worker_class option
            worker_class_name = self.opts.pop('worker_class')
        if worker_class_name is None:
            print "--worker-help requires --worker-class to be set too"
        else:
            worker_class = load_class_by_string(worker_class_name)
            print worker_class.__doc__
        sys.exit(0)


# This create the service, runnable on command line with twistd
class VumiServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    # the name of our plugin, this will be the subcommand for twistd
    # e.g. $ twistd -n start_worker --option1= ...
    tapname = "start_worker"
    # description, also for twistd
    description = "Start a Vumi worker"
    # what command line options does this service expose
    options = BasicSet

    def makeService(self, options):
        return VumiService(options)

# Announce the plugin as a service maker for twistd
# See: http://twistedmatrix.com/documents/current/core/howto/tap.html
serviceMaker = VumiServiceMaker()
