
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.workers.ttc.workers import ParticipantModel
from vumi.tests.utils import get_stubbed_worker
from vumi.message import Message

from vumi.workers.ttc import TtcGenericWorker

class ParticipantModelTest(UglyModelTestCase):
    
    def setUp(self):
        return self.setup_db(ParticipantModel)
    
    def tearDown(self):
        return self.shutdown_db()
    
    def test_create_and_get_item(self):

        def _txn(txn):
            self.assertEqual(0, ParticipantModel.count_rows(txn))
            ParticipantModel.create_item(txn,445654332)
            self.assertEqual(1, ParticipantModel.count_rows(txn))
            participant = ParticipantModel.get_items(txn)
            self.assertEqual(445654332, participant[0].phone_number)
        d = self.ri(_txn)
        
        return d

class TtcGenericWorkerTestCase(TestCase):
    
    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test'
        self.config = {'transport_name': self.transport_name,
                       'dbname': 'dbtest'}
        self.worker = get_stubbed_worker(TtcGenericWorker,
                                         config=self.config)
        self.broker = self.worker._amqp_client.broker
        yield self.worker.startWorker()
    
    @inlineCallbacks
    def tearDown(self):
        yield self.worker.stopWorker()
    
    @inlineCallbacks
    def send(self, msg, routing_suffix ='control'):
        routing_key = "%s.%s" % (self.transport_name, routing_suffix)
        self.broker.publish_message('vumi', routing_key, msg)
        yield self.broker.kick_delivery()
    
    @inlineCallbacks
    def test_consume_control_config_file(self):
        events = [
            ('config', Message.from_json('{"program":{"name":"M4H"}}'))
            ]
        for name, event in events:
            yield self.send(event,'control')
            self.assertEqual(self.worker.record, [(event)])