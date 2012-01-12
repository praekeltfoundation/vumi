# -*- test-case-name: vumi.tests.test_log -*-
import logging
from functools import partial

from twisted.python import log


msg = debug = partial(log.msg, logLevel=logging.DEBUG)
info = partial(log.msg, logLevel=logging.INFO)
warning = partial(log.msg, logLevel=logging.WARNING)
err = error = partial(log.err, logLevel=logging.ERROR)
critical = partial(log.err, logLevel=logging.CRITICAL)
