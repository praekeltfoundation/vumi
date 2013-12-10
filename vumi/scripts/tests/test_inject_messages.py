import StringIO
import json

from twisted.internet.defer import inlineCallbacks

from vumi.scripts.inject_messages import MessageInjector
from vumi.tests.helpers import VumiTestCase, WorkerHelper


class TestMessageInjector(VumiTestCase):

    DEFAULT_DATA = {
        'content': 'CODE2',
        'transport_type': 'sms',
        'to_addr': '1458',
        'message_id': '1',
        'from_addr': '1234',
    }

    def setUp(self):
        self.worker_helper = self.add_helper(WorkerHelper('sphex'))

    def get_worker(self, direction):
        return self.worker_helper.get_worker(MessageInjector, {
            'transport-name': 'sphex',
            'direction': direction,
        })

    def make_data(self, **kw):
        kw.update(self.DEFAULT_DATA)
        return kw

    def check_msg(self, msg, data):
        for key in data:
            self.assertEqual(msg[key], data[key])

    @inlineCallbacks
    def test_process_line_inbound(self):
        worker = yield self.get_worker('inbound')
        data = self.make_data()
        worker.process_line(json.dumps(data))
        [msg] = self.worker_helper.get_dispatched_inbound()
        self.check_msg(msg, data)

    @inlineCallbacks
    def test_process_line_outbound(self):
        worker = yield self.get_worker('outbound')
        data = self.make_data()
        worker.process_line(json.dumps(data))
        [msg] = self.worker_helper.get_dispatched_outbound()
        self.check_msg(msg, data)

    @inlineCallbacks
    def test_process_file_inbound(self):
        worker = yield self.get_worker('inbound')
        data = [self.make_data(message_id=i) for i in range(10)]
        data_string = "\n".join(json.dumps(datum) for datum in data)
        in_file = StringIO.StringIO(data_string)
        out_file = StringIO.StringIO()
        yield worker.process_file(in_file, out_file)
        msgs = self.worker_helper.get_dispatched_inbound()
        for msg, datum in zip(msgs, data):
            self.check_msg(msg, datum)
        self.assertEqual(out_file.getvalue(), data_string + "\n")

    @inlineCallbacks
    def test_process_file_outbound(self):
        worker = yield self.get_worker('outbound')
        data = [self.make_data(message_id=i) for i in range(10)]
        data_string = "\n".join(json.dumps(datum) for datum in data)
        in_file = StringIO.StringIO(data_string)
        out_file = StringIO.StringIO()
        yield worker.process_file(in_file, out_file)
        msgs = self.worker_helper.get_dispatched_outbound()
        for msg, datum in zip(msgs, data):
            self.check_msg(msg, datum)
        self.assertEqual(out_file.getvalue(), data_string + "\n")
