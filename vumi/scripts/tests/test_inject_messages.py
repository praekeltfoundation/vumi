from twisted.internet.defer import inlineCallbacks
from vumi.transports.tests.test_base import TransportTestCase
from vumi.scripts.inject_messages import MessageInjector
import json
import StringIO


class MessageInjectorTestCase(TransportTestCase):

    transport_class = MessageInjector

    DEFAULT_DATA = {
            'content': 'CODE2',
            'transport_type': 'sms',
            'to_addr': '1458',
            'message_id': '1',
            'from_addr': '1234',
        }

    @inlineCallbacks
    def setUp(self):
        super(MessageInjectorTestCase, self).setUp()
        self.transport = yield self.get_transport({
            'transport-name': 'test_transport',
        })

    def make_data(self, **kw):
        kw.update(self.DEFAULT_DATA)
        return kw

    def check_msg(self, msg, data):
        for key in data:
            self.assertEqual(msg[key], data[key])

    def test_process_line(self):
        data = self.make_data()
        self.transport.process_line(json.dumps(data))
        [msg] = self._amqp.get_messages('vumi', 'test_transport.inbound')
        self.check_msg(msg, data)

    @inlineCallbacks
    def test_process_file(self):
        data = [self.make_data(message_id=i) for i in range(10)]
        data_string = "\n".join(json.dumps(datum) for datum in data)
        in_file = StringIO.StringIO(data_string)
        out_file = StringIO.StringIO()
        yield self.transport.process_file(in_file, out_file)
        msgs = self._amqp.get_messages('vumi', 'test_transport.inbound')
        for msg, datum in zip(msgs, data):
            self.check_msg(msg, datum)
        self.assertEqual(out_file.getvalue(), data_string + "\n")
