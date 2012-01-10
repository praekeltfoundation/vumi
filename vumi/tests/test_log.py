import logging

from twisted.trial.unittest import TestCase
from vumi.log import _mk_logger


class VumiLogTestCase(TestCase):

    def setUp(self):
        self._log_history = []

    def _log_callback(self, msg, **kwargs):
        self._log_history.append((msg, kwargs))

    def last_log_call(self):
        return self._log_history[-1]

    def test_invalid_log_level(self):
        self.assertRaises(RuntimeError, _mk_logger, 'NOT_A_LEVEL',
                            self._log_callback)

    def test_logging(self):
        levels = [
            'DEBUG',
            'INFO',
            'WARNING',
            'ERROR',
            'CRITICAL'
        ]
        for level in levels:
            logger = _mk_logger(level, self._log_callback)
            logger('foo %s' % (level,))
            self.assertEqual(self.last_log_call(), (
                '%s: foo %s' % (level, level), {
                    'logLevel': getattr(logging, level)
                }
            ))