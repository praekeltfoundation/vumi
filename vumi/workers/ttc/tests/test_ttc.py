
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo
from datetime import datetime, time, date, timedelta

#from vumi.database.tests.test_base import UglyModelTestCase
#from vumi.workers.ttc.workers import ParticipantModel
from vumi.tests.utils import get_stubbed_worker
from vumi.message import Message

from vumi.workers.ttc import TtcGenericWorker

from vumi.message import TransportUserMessage

#class ParticipantModelTest(UglyModelTestCase):
    
    #def setUp(self):
        #return self.setup_db(ParticipantModel)
    
    #def tearDown(self):
        #return self.shutdown_db()
    
    
    #def test_create_and_get_item(self):
        #self.assertTrue(True)
#        def _txn(txn):
#            self.assertEqual(0, ParticipantModel.count_rows(txn))
#            ParticipantModel.create_item(txn,445654332)
#            self.assertEqual(1, ParticipantModel.count_rows(txn))
#            participant = ParticipantModel.get_items(txn)
#            self.assertEqual(445654332, participant[0].phone_number)
#        d = self.ri(_txn)
        
#        return d

class FakeUserMessage(TransportUserMessage):
    def __init__(self, **kw):
        kw['to_addr'] = 'to'
        kw['from_addr'] = 'from'
        kw['transport_name'] = 'test'
        kw['transport_type'] = 'fake'
        kw['transport_metadata'] = {}
        super(FakeUserMessage, self).__init__(**kw)

class TestTtcGenericWorker(TestCase):
    
    @inlineCallbacks
    def setUp(self):
        self.transport_name = 'test'
        self.config = {'transport_name': self.transport_name,
                       'dbname': 'dbtest'}
        self.worker = get_stubbed_worker(TtcGenericWorker,
                                         config=self.config)        
        self.broker = self.worker._amqp_client.broker
        
        self.broker.exchange_declare('vumi','direct')
        self.broker.queue_declare("%s.outbound" % self.transport_name)
        self.broker.queue_bind("%s.outbound" % self.transport_name, "vumi","%s.outbound" % self.transport_name)
        
        #Database#
        connection = pymongo.Connection("localhost",27017)
        self.db = connection.test
        self.programs = self.db.programs
        
        #Let's rock"
        yield self.worker.startWorker()
    
    @inlineCallbacks
    def tearDown(self):
        self.db.programs.drop()
        self.db.schedules.drop()
        yield self.worker.stopWorker()
    
    @inlineCallbacks
    def send(self, msg, routing_suffix ='control'):
        routing_key = "%s.%s" % (self.transport_name, routing_suffix)
        self.broker.publish_message('vumi', routing_key, msg)
        yield self.broker.kick_delivery()
 
    #@inlineCallbacks
    #def test_consume_user_message(self):
        #self.assertTrue(True)
        #messages = [
            #('user_message', FakeUserMessage()),
            #('new_session', FakeUserMessage(session_event=SESSION_NEW)),
            #('close_session', FakeUserMessage(session_event=SESSION_CLOSE)),
            #]
        #for name, message in messages:
            #yield self.send(message,"outbound")
    
    @inlineCallbacks
    def test_consume_control_program(self):
        events = [
            #('config', Message.from_json('{"program":{"name":"M4H"}}'))
            #('config', Message.from_json('{"program":{"name":"M4H","dialogues":{"name":"main"}}}'))
            #('config', Message.from_json('{"program":{"name":"M4H","dialogues":[{"name":"main","type":"sequential","interactions":[{"type":"announcement","content":"Hello","date":"today","time":"now"}]}]}}'))
            ('config', Message.from_json("""{"program":{"name":"M5H","dialogues":
            [{"name":"main","type":"sequential","interactions":[
            {"type":"announcement","name":"1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","name":"2","content":"How are you","schedule_type":"wait(00:20)"}
            ]}
            ]}}"""))
        ]
        for name, event in events:
            yield self.send(event,'control')
        self.assertEqual(self.programs.count(), 1)
        
    @inlineCallbacks
    def test_consume_control_participant(self):
        events = [
            ('1', Message.from_json("""{"program":{"name":"M4H","dialogues":
            [{"name":"main","type":"sequential","interactions":[
            {"type":"announcement","name":"1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","name":"2","content":"How are you","schedule_type":"wait(00:20)"}
            ]}
            ]}}""")),
            ('2', Message.from_json("""{"participants":[
            {"phone":"788601462"},
            {"phone":"788601463"}
            ]}"""))
            ]
        events.sort()
        for name, event in events:
            yield self.send(event,'control')
        program = self.programs.find_one({"name":"M4H"})
        participants = program.get("participants")
        self.assertEqual(participants, events[1][1].get('participants'))
    
    
    def test_schedule_participant_dialogue(self):
        dialogue = {"name":"main","type":"sequential","interactions":[
            {"type":"announcement","name":"i1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","name":"i2","content":"How are you","schedule_type":"wait"}
            ]}
        participant = {"phone":"09984", "name":"olivier"}
        
        schedules = self.worker.schedule_participant_dialogue(participant, dialogue)
        #assert time calculation
        self.assertEqual(len(schedules), 2)
        self.assertTrue(datetime.strptime(schedules[0].get("datetime"),"%Y-%m-%dT%H:%M:%S.%f") - datetime.now() < timedelta(seconds=1))
        self.assertTrue(datetime.strptime(schedules[1].get("datetime"),"%Y-%m-%dT%H:%M:%S.%f") - datetime.now() < timedelta(minutes=10))
        self.assertTrue(datetime.strptime(schedules[1].get("datetime"),"%Y-%m-%dT%H:%M:%S.%f") - datetime.now() > timedelta(minutes=9))
        
        #assert schedule links
        self.assertEqual(schedules[0].get("participant_phone"),"09984")
        self.assertEqual(schedules[0].get("dialogue_name"),"main")
        self.assertEqual(schedules[0].get("interaction_name"),"i1")
        self.assertEqual(schedules[1].get("interaction_name"),"i2")
    
    @inlineCallbacks
    def test_send_scheduled_oneMessage(self):
        dNow = datetime.now()
        self.worker.db.schedules.save({"datetime":dNow.isoformat(),
                                       "dialogue_name": "main",
                                       "interaction_name": "int1",
                                       "participant_phone": "09"});
        yield self.worker.send_scheduled()
        message = self.broker.basic_get('%s.outbound' % self.transport_name)[1].get('content')
        message = TransportUserMessage.from_json( message)
        self.assertEqual(message.payload['to_addr'], "09")
        #self.assertEqual(message.payload['from_addr'], "8282")
        #self.assertEqual(message.payload['content'],"Hello Olivier")
        self.assertEqual(self.worker.db.schedules.count(), 0)
    
    def test_send_scheduled_chooseMessage(self):
        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes = 30)
        dFuture = datetime.now() + timedelta(minutes = 30)
