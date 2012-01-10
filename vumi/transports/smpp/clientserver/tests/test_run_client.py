
from twisted.trial import unittest

from vumi.transports.smpp.clientserver.client import ESME, KeyValueStore


class EsmeTestCase(unittest.TestCase):

    def setUp(self):
        self.esme = ESME(
                None,
                KeyValueStore(),
                )

    def test_esme_init_kvs_test(self):
        pass
