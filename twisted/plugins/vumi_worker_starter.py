import yaml
from zope.interface import implements
from twisted.python import log
from twisted.application.service import IServiceMaker, Service
from twisted.plugin import IPlugin

from vumi.service import Options, WorkerCreator
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
        global_options = {}
        for opt in [i[0] for i in Options.optParameters]:
            global_options[opt] = self.options.pop(opt)

        worker_creator = WorkerCreator(global_options)

        # We need an initial worker. This would either be a normal
        # worker class (if we're using the old one-per-process model)
        # or a worker that loads new workers.
        worker_class = self.options.pop("worker_class")
        if not worker_class:
            raise VumiError, "please specify --worker_class"

        config = {}
        # load the config file if specified
        config_file = self.options.pop('config')
        if config_file:
            with file(config_file, 'r') as stream:
                config.update(yaml.load(stream))

        for k, v in self.options.items():
            if k.startswith("config_"):
                config[k[7:]] = v

        worker_creator.create_worker(worker_class, config)

    # Twistd calls this method at shutdown
    def stopService(self):
        log.msg("Stopping VumiService")



# Extend the default Vumi options with whatever options your service needs
class BasicSet(Options):
    optParameters = Options.optParameters + [
        ["worker_class", None, None, "class of a worker to start"],
        ["config", None, None, "YAML config file to load"],
        ["config_smpp_increment", None, 1, "Increment for SMPP sequence number (must be >= number of SMPP workers on a single SMPP account)"],
        ["config_smpp_offset", None, 1, "Offset for this worker's SMPP sequence numbers (no duplicates on a single SMPP account and must be <= increment)"],
    ]

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
