from zope.interface import implements
from twisted.application.service import IServiceMaker
from twisted.plugin import IPlugin

from vumi.servicemaker import VumiService, StartWorkerOptions


# This create the service, runnable on command line with twistd
class VumiServiceMaker(object):
    implements(IServiceMaker, IPlugin)
    # the name of our plugin, this will be the subcommand for twistd
    # e.g. $ twistd -n vumi_worker --option1= ...
    tapname = "vumi_worker"
    # description, also for twistd
    description = "Start a Vumi worker"
    # what command line options does this service expose
    options = StartWorkerOptions

    def makeService(self, options):
        return VumiService(options)


class DeprecatedServiceMaker(VumiServiceMaker):
    tapname = "start_worker"
    description = "Deprecated copy of vumi_worker. Use vumi_worker instead."

# Announce the plugin as a service maker for twistd
# See: http://twistedmatrix.com/documents/current/core/howto/tap.html
serviceMaker = VumiServiceMaker()
deprecatedMaker = DeprecatedServiceMaker()
