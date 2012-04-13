"""Plugins for starting Vumi workers from twistd."""

from vumi.servicemaker import (VumiWorkerServiceMaker,
                               DeprecatedStartWorkerServiceMaker)

# Having instances of IServiceMaker present magically announces the
# service makers to twistd.
# See: http://twistedmatrix.com/documents/current/core/howto/tap.html
vumi_worker = VumiWorkerServiceMaker()
start_worker = DeprecatedStartWorkerServiceMaker()
