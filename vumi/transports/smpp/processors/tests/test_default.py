from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import FailTest

from vumi.errors import ConfigError
from vumi.tests.helpers import VumiTestCase
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.smpp.smpp_transport import SmppTransceiverTransport
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC


class DefaultProcessorTestCase(VumiTestCase):
    def setUp(self):
        self.fake_smsc = FakeSMSC()
        self.tx_helper = self.add_helper(
            TransportHelper(SmppTransceiverTransport))

    @inlineCallbacks
    def test_data_coding_override_keys_ints(self):
        """
        If the keys of the data coding overrides config dictionary are not
        integers, they should be cast to integers.
        """
        config = {
            'system_id': 'foo',
            'password': 'bar',
            'twisted_endpoint': self.fake_smsc.endpoint,
            'deliver_short_message_processor_config': {
                'data_coding_overrides': {
                    '0': 'utf-8'
                },
            },
        }
        transport = yield self.tx_helper.get_transport(config)
        self.assertEqual(
            transport.deliver_sm_processor.data_coding_map.get(0), 'utf-8')

    @inlineCallbacks
    def test_data_coding_override_keys_invalid(self):
        """
        If the keys of the data coding overrides config dictionary can not be
        cast to integers, a config error with an appropriate message should
        be raised.
        """
        config = {
            'system_id': 'foo',
            'password': 'bar',
            'twisted_endpoint': self.fake_smsc.endpoint,
            'deliver_short_message_processor_config': {
                'data_coding_overrides': {
                    'not-an-int': 'utf-8'
                },
            },
        }
        try:
            yield self.tx_helper.get_transport(config)
        except ConfigError as e:
            self.assertEqual(
                str(e),
                "data_coding_overrides keys must be castable to ints. "
                "invalid literal for int() with base 10: 'not-an-int'"
            )
        else:
            raise FailTest("Expected ConfigError to be raised")
