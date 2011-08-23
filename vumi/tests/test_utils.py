import os.path

from twisted.trial.unittest import TestCase

from vumi.utils import (normalize_msisdn, make_vumi_path_abs, cleanup_msisdn,
                        get_operator_name)


class UtilsTestCase(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_normalize_msisdn(self):
        self.assertEqual(normalize_msisdn('0761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('27761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('+27761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('0027761234567', '27'),
                         '+27761234567')
        self.assertEqual(normalize_msisdn('1234'), '1234')
        self.assertEqual(normalize_msisdn('12345'), '12345')
        self.assertEqual(normalize_msisdn('+12345'), '+12345')

    def test_make_campaign_path_abs(self):
        vumi_tests_path = os.path.dirname(__file__)
        vumi_path = os.path.dirname(os.path.dirname(vumi_tests_path))
        self.assertEqual('/foo/bar', make_vumi_path_abs('/foo/bar'))
        self.assertEqual(os.path.join(vumi_path, 'foo/bar'),
                         make_vumi_path_abs('foo/bar'))

    def test_cleanup_msisdn(self):
        self.assertEqual('27761234567', cleanup_msisdn('27761234567', '27'))
        self.assertEqual('27761234567', cleanup_msisdn('+27761234567', '27'))
        self.assertEqual('27761234567', cleanup_msisdn('0761234567', '27'))

    def test_get_operator_name(self):
        mapping = {'27': {'2782': 'VODACOM', '2783': 'MTN'}}
        self.assertEqual('MTN', get_operator_name('27831234567', mapping))
        self.assertEqual('VODACOM', get_operator_name('27821234567', mapping))
        self.assertEqual('UNKNOWN', get_operator_name('27801234567', mapping))
