import logging

from vumi.tests.utils import LogCatcher
from vumi import log
from vumi.log import WrappingLogger
from vumi.tests.helpers import VumiTestCase


class TracerException(Exception):
    pass


class TestVumiLog(VumiTestCase):

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
        self.add_cleanup(self.flushLoggedErrors, TracerException)
        for label, logger in levels:
            entry = 'foo %s' % (label,)
            lc = LogCatcher()
            with lc:
                logger(TracerException(entry))
            entry = lc.logs[0]
            self.assertTrue(entry['isError'])
            self.assertEqual(entry['logLevel'], getattr(logging, label))
            self.assertEqual(entry['message'], ())
            failure = entry['failure']
            exception = failure.trap(TracerException)
            self.assertEqual(exception, TracerException)


class TestWrappingLogger(VumiTestCase):
    def test_logging_methods(self):
        log = WrappingLogger(system='test', bar='foo')

        for method, level in (
                (log.debug, logging.DEBUG),
                (log.info, logging.INFO),
                (log.warning, logging.WARNING),
                (log.error, logging.ERROR),
                (log.critical, logging.CRITICAL),
                (log.msg, logging.INFO),
                (log.err, logging.ERROR)):
            with LogCatcher() as lc:
                method('Test log')
            [log] = lc.logs

            self.assertEqual(log['system'], 'test')
            self.assertEqual(log['bar'], 'foo')
            self.assertEqual(log['logLevel'], level)

