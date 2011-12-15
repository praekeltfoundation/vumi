
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
        self.control_name = 'mycontrol'
        self.config = {'transport_name': self.transport_name,
                       'database': 'test',
                       'control_name':'mycontrol'}
        self.worker = get_stubbed_worker(TtcGenericWorker,
                                         config=self.config)        
        self.broker = self.worker._amqp_client.broker
        
        self.broker.exchange_declare('vumi','direct')
        self.broker.queue_declare("%s.outbound" % self.transport_name)
        self.broker.queue_bind("%s.outbound" % self.transport_name, "vumi","%s.outbound" % self.transport_name)
        
        #Database#
        connection = pymongo.Connection("localhost",27017)
        self.db = connection[self.config['database']]
        self.programs = self.db.programs
        
        #Let's rock"
        yield self.worker.startWorker()
    
    @inlineCallbacks
    def tearDown(self):
        self.db.programs.drop()
        if (self.worker.program_name):
            self.worker.collection_schedules.drop()
            self.worker.collection_logs.drop()
        if (self.worker.sender != None):
            yield self.worker.sender.stop()
        yield self.worker.stopWorker()
    
    @inlineCallbacks
    def send(self, msg, routing_suffix ='control'):
        if (routing_suffix=='control'):
            routing_key = "%s.%s" % (self.control_name, routing_suffix)
        else:
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
            ('config', Message.from_json("""{"program":{"name":"M5H", "shortcode":"8282","participants":[{"phone":"06"}],
            "dialogues":
            [{"dialogue_id":"0","type":"sequential","interactions":[
            {"type":"announcement","interaction_id":"1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","interaction_id":"2","content":"How are you","schedule_type":"wait(00:20)"}
            ]}
            ]}}"""))
        ]
        for name, event in events:
            yield self.send(event,'control')
        self.assertEqual(self.programs.count(), 1)
        self.assertEqual(self.worker.collection_logs.count(),2)
        
    @inlineCallbacks
    def test_consume_control_participant(self):
        events = [
            ('1', Message.from_json("""{"program":{"name":"M4H","shortcode":"8282","dialogues":
            [{"dialogue_id":"0","type":"sequential","interactions":[
            {"type":"announcement","interaction_id":"1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","interaction_id":"2","content":"How are you","schedule_type":"wait(00:20)"}
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
        program = {"name":"M5H", "participants":[{"phone":"09984", "name":"olivier"}],"dialogues" : 
                   [{"dialogue_id":"0","type":"sequential","interactions":[
                       {"type":"announcement","interaction_id":"1","content":"Hello","schedule_type":"immediately"},
                       {"type":"announcement","interaction_id":"2","content":"How are you","schedule_type":"wait"}
                   ]}]}

        self.worker.init_program(program)
        
        schedules = self.worker.schedule_participant_dialogue(program['participants'][0], program['dialogues'][0])
        #assert time calculation
        self.assertEqual(len(schedules), 2)
        self.assertTrue(datetime.strptime(schedules[0].get("datetime"),"%Y-%m-%dT%H:%M:%S.%f") - datetime.now() < timedelta(seconds=1))
        self.assertTrue(datetime.strptime(schedules[1].get("datetime"),"%Y-%m-%dT%H:%M:%S.%f") - datetime.now() < timedelta(minutes=2))
        self.assertTrue(datetime.strptime(schedules[1].get("datetime"),"%Y-%m-%dT%H:%M:%S.%f") - datetime.now() > timedelta(minutes=1))
        
        #assert schedule links
        self.assertEqual(schedules[0].get("participant_phone"),"09984")
        self.assertEqual(schedules[0].get("dialogue_id"),"0")
        self.assertEqual(schedules[0].get("interaction_id"),"1")
        self.assertEqual(schedules[1].get("interaction_id"),"2")
    
    @inlineCallbacks
    def test_send_scheduled_oneMessage(self):
        dNow = datetime.now()
        program = {"name":"M5H","shortcode":"8282","dialogues":
            [{"dialogue_id":"0","type":"sequential","interactions":[
            {"type":"announcement","interaction_id":"1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","interaction_id":"2","content":"How are you","schedule_type":"wait(00:20)"}
            ]}
            ]}
        self.worker.init_program(program)
        self.worker.collection_schedules.save({"datetime":dNow.isoformat(),
                                       "dialogue_id": "0",
                                       "interaction_id": "1",
                                       "participant_phone": "09"});
        yield self.worker.send_scheduled()
        message = self.broker.basic_get('%s.outbound' % self.transport_name)[1].get('content')
        message = TransportUserMessage.from_json( message)
        self.assertEqual(message.payload['to_addr'], "09")
        #self.assertEqual(message.payload['from_addr'], "8282")
        self.assertEqual(message.payload['content'],"Hello")
        self.assertEqual(self.worker.collection_schedules.count(), 0)
    
    @inlineCallbacks
    def test_send_scheduled_only_in_past(self):
        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes = 30)
        dFuture = datetime.now() + timedelta(minutes = 30)
        program = ({"name":"M5H","shortcode":"8282","dialogues":
            [{"dialogue_id":"0","type":"sequential","interactions":[
            {"type":"announcement","interaction_id":"1","content":"Hello","schedule_type":"immediately"},
            {"type":"announcement","interaction_id":"2","content":"Today will be sunny","schedule_type":"wait-20"},
            {"type":"announcement","interaction_id":"3","content":"Today is the special day","schedule_type":"wait-20"}           
            ]}
            ]})
        self.worker.init_program(program)
        self.worker.collection_schedules.save({"datetime":dPast.isoformat(),
                                       "dialogue_id": "0",
                                       "interaction_id": "1",
                                       "participant_phone": "09"});
        self.worker.collection_schedules.save({"datetime":dNow.isoformat(),
                                       "dialogue_id": "0",
                                       "interaction_id": "2",
                                       "participant_phone": "09"});
        self.worker.collection_schedules.save({"datetime":dFuture.isoformat(),
                                       "dialogue_id": "0",
                                       "interaction_id": "3",
                                       "participant_phone": "09"});
        yield self.worker.send_scheduled()
        #first message is the oldest
        message1 = TransportUserMessage.from_json( self.broker.basic_get('%s.outbound' % self.transport_name)[1].get('content'))
        self.assertEqual(message1.payload['content'],"Hello")
        #second message
        message2 = TransportUserMessage.from_json( self.broker.basic_get('%s.outbound' % self.transport_name)[1].get('content'))
        self.assertEqual(message2.payload['content'],"Today will be sunny")   
        #third message is not send, so still in the schedules collection and two messages in the logs collection
        self.assertEquals(self.worker.collection_schedules.count(),1)
        self.assertEquals(self.worker.collection_logs.count(),2)
        self.assertTrue(self.broker.basic_get('%s.outbound' % self.transport_name))    
        #only two message should be send
        self.assertTrue((None,None) == self.broker.basic_get('%s.outbound' % self.transport_name))