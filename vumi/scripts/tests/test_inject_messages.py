from twisted.internet.defer import inlineCallbacks
from vumi.transports.tests.test_base import TransportTestCase
from vumi.scripts.inject_messages import MessageInjector
import json


class MessageInjectorTestCase(TransportTestCase):

    transport_class = MessageInjector

    @inlineCallbacks
    def test_injecting(self):
        transport = yield self.get_transport({
            'transport-name': 'test_transport',
        })

        data = {
            'content': 'CODE2',
            'transport_type': 'sms',
            'to_addr': '1458',
            'message_id': '1',
            'from_addr': '1234',
        }
        transport.process_line(json.dumps(data))
        [msg] = self._amqp.get_messages('vumi', 'test_transport.inbound')
        self.assertEqual(msg['content'], 'CODE2')
        self.assertEqual(msg['transport_type'], 'sms')
        self.assertEqual(msg['to_addr'], '1458')
        self.assertEqual(msg['from_addr'], '1234')
        self.assertEqual(msg['message_id'], '1')
