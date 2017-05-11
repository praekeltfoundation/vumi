from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import Clock
from twisted.trial.unittest import FailTest

from vumi.errors import ConfigError
from vumi.tests.helpers import VumiTestCase
from vumi.transports.tests.helpers import TransportHelper
from vumi.transports.smpp.pdu_utils import unpacked_pdu_opts
from vumi.transports.smpp.smpp_transport import SmppTransceiverTransport
from vumi.transports.smpp.tests.fake_smsc import FakeSMSC


class DefaultProcessorTestCase(VumiTestCase):
    def setUp(self):
        self.fake_smsc = FakeSMSC()
        self.tx_helper = self.add_helper(
            TransportHelper(SmppTransceiverTransport))
        self.clock = Clock()

    @inlineCallbacks
    def get_transport(self, config):
        transport = yield self.tx_helper.get_transport(config, start=False)
        transport.clock = self.clock
        yield transport.startWorker()
        self.clock.advance(0)
        yield self.fake_smsc.bind()
        returnValue(transport)

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

    @inlineCallbacks
    def test_multipart_sar_reference_rollover(self):
        """
        If the multipart_sar_reference_rollover config value is set, then for
        multipart messages, the reference should rollover at that value.
        """
        config = {
            'system_id': 'foo',
            'password': 'bar',
            'twisted_endpoint': self.fake_smsc.endpoint,
            'submit_short_message_processor_config': {
                'send_multipart_sar': True,
                'multipart_sar_reference_rollover': 0xFF,
            },
        }
        transport = yield self.get_transport(config)
        transport.service.sequence_generator.redis.set(
            'smpp_last_sequence_number', 0xFF)

        yield transport.submit_sm_processor.send_short_message(
            transport.service, 'test-id', '+1234', 'test message ' * 20,
            optional_parameters={})
        pdus = yield self.fake_smsc.await_pdus(2)

        msg_refs = [unpacked_pdu_opts(p)['sar_msg_ref_num'] for p in pdus]
        self.assertEqual(msg_refs, [1, 1])
