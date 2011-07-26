from twisted.trial.unittest import TestCase
from vumi.utils import normalize_msisdn

class UtilsTestCase(TestCase):
    def setUp(self):
        pass
    
    def tearDown(self):
        pass
    
    def test_normalize_msisdn(self):
        self.assertEquals(normalize_msisdn('0761234567', '27'), '+27761234567')
        self.assertEquals(normalize_msisdn('27761234567', '27'), '+27761234567')
        self.assertEquals(normalize_msisdn('+27761234567', '27'), '+27761234567')
        self.assertEquals(normalize_msisdn('0027761234567', '27'), '+27761234567')
        self.assertEquals(normalize_msisdn('1234'), '1234')
        self.assertEquals(normalize_msisdn('12345'), '12345')
        self.assertEquals(normalize_msisdn('+12345'), '+12345')
    