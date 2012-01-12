# -*- test-case-name: vumi.tests.test_log -*-
import logging
from functools import partial

from twisted.python import log


debug = partial(log.msg, logLevel=logging.DEBUG)
info = partial(log.msg, logLevel=logging.INFO)
warning = partial(log.msg, logLevel=logging.WARNING)
error = partial(log.err, logLevel=logging.ERROR)
critical = partial(log.err, logLevel=logging.CRITICAL)

# make transition from twisted.python.log easier
msg = info
err = error
