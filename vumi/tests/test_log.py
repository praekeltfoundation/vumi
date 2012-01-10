import logging

from twisted.trial.unittest import TestCase
from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet import reactor

from vumi.tests.utils import LogCatcher
from vumi import log


class TestException(Exception): pass

class VumiLogTestCase(TestCase):

    def setUp(self):
        self._log_history = []

    def test_invalid_log_level(self):
        self.assertRaises(RuntimeError, log._mk_logger, 'NOT_A_LEVEL',
                            lambda *a, **kw: None)

    def test_normal_log_levels(self):
        levels = [
            ('DEBUG', log.debug),
            ('INFO', log.info),
            ('WARNING', log.warning),
        ]
        for label, logger in levels:
            log_catcher = LogCatcher()
            with log_catcher:
                logger('foo %s' % (label,))
            last_log = log_catcher.logs[0]
            self.assertFalse(last_log['isError'])
            self.assertEqual(last_log['logLevel'], getattr(logging, label))
            self.assertEqual(last_log['message'],
                                ('foo %s' % (label,),))

    def test_error_log_levels(self):
        levels = [
            ('ERROR', log.error),
            ('CRITICAL', log.critical),
        ]
        for label, logger in levels:
            entry = 'foo %s' % (label,)
            lc = LogCatcher()
            with lc:
                logger(TestException(entry))
            entry = lc.logs[0]
            self.assertTrue(entry['isError'])
            self.assertEqual(entry['logLevel'], getattr(logging, label))
            self.assertEqual(entry['message'], ())
            failure = entry['failure']
            exception = failure.trap(TestException)
            self.assertEqual(exception, TestException)
