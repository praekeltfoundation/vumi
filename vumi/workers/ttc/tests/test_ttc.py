
from twisted.trial.unittest import TestCase
from twisted.internet.defer import inlineCallbacks

import pymongo
import json
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
    
    emptyProgram = """
    {"program":{
            "name":"M5H"}
    }
    """
    
    simpleProgram= """{"program":{
            "name":"M5H", 
            "shortcode":"8282",
            "participants":[{"phone":"06"}],
            "dialogues":
            [{"dialogue-id":"0","interactions":[
                   {"type-interaction":"announcement",
                   "interaction-id":"0",
                   "content":"Hello",
                   "type-schedule":"immediately"},
                   {"type-interaction":"announcement",
                   "interaction-id":"1",
                   "content":"How are you",
                   "type-schedule":"wait",
                   "minutes":"60"}]}
            ]}}"""
    
    simpleParticipant = """{"participants":[
            {"phone":"788601462"},
            {"phone":"788601463"}
            ]}"""

    controlMessage="""
    {
        "action":"",
        "content":""
    }"""
    
    simpleProgram_Question = """
    {"program": {
		"name": "QuestionDialogue",
		"dialogues": [
			{
				"interactions": [
					{
						"type-interaction": "question-answer",
						"content": "How are you?",
						"answers": [
							{
								"choice": "Fine"
							},
							{
								"choice": "Ok"
							}
						],
						"type-schedule": "immediately"
					}
				]
			}
		]
	}
    }"""
    
    simpleProgram_announcement_fixedtime = """
    {"program": {
            "participants": [
                    {
                            "phone": "06"
                    }
            ],
            "dialogues": [
                    {
                            "dialogue-id":"program.dialogues[0]",
                            "interactions": [
                                    {
                                            "interaction-id":"0",
                                            "type-interaction": "announcement",
                                            "content": "Hello",
                                            "type-schedule": "fixed-time",
                                            "day": "2",
                                            "month":"3",
                                            "year":"2018",
                                            "time": "12:30"
                                    }
                            ]
                    }
            ]
    }}"""    
    
    simpleAnnouncement = """
    {
            "type-interaction": "announcement",
            "content": "Hello",
            "type-schedule": "wait",
            "time": "02:30"
    }"""
    
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
    def test01_consume_control_program(self):
        events = [
            ('config', Message.from_json(self.emptyProgram))
        ]
        self.programs.drop()
        for name, event in events:
            yield self.send(event,'control')
        self.assertEqual(self.programs.count(), 1)
        #self.assertEqual(self.worker.collection_logs.count(),1)
        #self.assertEqual(self.worker.collection_schedules.count(),1)
        
    #@inlineCallbacks
    #def test02_add_participant(self):
        #events = [
            #('1', Message.from_json(self.simpleProgram)),
            #('2', Message.from_json(self.simpleParticipant))
            #]
        #events.sort()
        #for name, event in events:
            #yield self.send(event,'control')
        #program = self.programs.find_one({"name":"M5H"})
        #participants = program.get("participants")
        #self.assertEqual(participants, 
                         #events[0][1].get('program').get('participants') + 
                         #events[1][1].get('participants'))
    
    
    def test03_schedule_participant_dialogue(self):
        program = Message.from_json(self.simpleProgram)['program']
        
        self.worker.init_program_db(program['name'])
        self.worker.db.programs.save(program)
        
        schedules = self.worker.schedule_participant_dialogue(program['participants'][0], program['dialogues'][0])
        #assert time calculation
        self.assertEqual(len(schedules), 2)
        self.assertTrue(datetime.strptime(schedules[0].get("datetime"),"%Y-%m-%dT%H:%M:%S") - datetime.now() < timedelta(seconds=1))
        self.assertTrue(datetime.strptime(schedules[1].get("datetime"),"%Y-%m-%dT%H:%M:%S") - datetime.now() < timedelta(minutes=60))
        self.assertTrue(datetime.strptime(schedules[1].get("datetime"),"%Y-%m-%dT%H:%M:%S") - datetime.now() > timedelta(minutes=59))
        
        #assert schedule links
        self.assertEqual(schedules[0].get("participant-phone"),"06")
        self.assertEqual(schedules[0].get("dialogue-id"),"0")
        self.assertEqual(schedules[0].get("interaction-id"),"0")
        self.assertEqual(schedules[1].get("interaction-id"),"1")
    
    @inlineCallbacks
    def test04_send_scheduled_oneMessage(self):
        dNow = datetime.now()
        program = Message.from_json(self.simpleProgram)['program']
    
        self.worker.init_program_db(program['name'])
        self.worker.db.programs.save(program)
        
        self.worker.collection_schedules.save({"datetime":dNow.isoformat(),
                                       "dialogue-id": "0",
                                       "interaction-id": "0",
                                       "participant-phone": "09"});
        yield self.worker.send_scheduled()
        message = self.broker.basic_get('%s.outbound' % self.transport_name)[1].get('content')
        message = TransportUserMessage.from_json( message)
        self.assertEqual(message.payload['to_addr'], "09")
        self.assertEqual(message.payload['content'], "Hello")
        self.assertEqual(self.worker.collection_schedules.count(), 0)
        log = self.worker.collection_logs.find_one()
        self.assertEquals(log['participant-phone'], "09")
        self.assertEquals(log['dialogue-id'], "0")
        self.assertEquals(log['interaction-id'], "0")
    
    @inlineCallbacks
    def test05_send_scheduled_only_in_past(self):
        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes = 30)
        dFuture = datetime.now() + timedelta(minutes = 30)
        program = ({"name":"M5H","shortcode":"8282","dialogues":
            [{"dialogue-id":"0","type":"sequential","interactions":[
            {"type":"announcement","interaction-id":"0","content":"Hello","schedule-type":"immediately"},
            {"type":"announcement","interaction-id":"1","content":"Today will be sunny","schedule-type":"wait-20"},
            {"type":"announcement","interaction-id":"2","content":"Today is the special day","schedule-type":"wait-20"}           
            ]}
            ]})
        
        self.worker.init_program_db(program['name'])
        self.worker.db.programs.save(program)
        
        self.worker.collection_schedules.save({"datetime":dPast.isoformat(),
                                       "dialogue-id": "0",
                                       "interaction-id": "0",
                                       "participant-phone": "09"});
        self.worker.collection_schedules.save({"datetime":dNow.isoformat(),
                                       "dialogue-id": "0",
                                       "interaction-id": "1",
                                       "participant-phone": "09"});
        self.worker.collection_schedules.save({"datetime":dFuture.isoformat(),
                                       "dialogue-id": "0",
                                       "interaction-id": "2",
                                       "participant-phone": "09"});
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
       
       
    def getCollection(self, db, collection_name):
        if ( collection_name in self.db.collection_names()):
            return db[collection_name]
        else:
            return db.create_collection(collection_name)

     
    def test06_schedule_interaction_while_interaction_inlog(self):
        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes = 30)
        dFuture = datetime.now() + timedelta(minutes = 30)
        
        program_name = "M5H"
        
        #set up of the data
        connection = pymongo.Connection("localhost",27017)
        self.db = connection['test']
        
        program = json.loads(self.simpleProgram)['program']
        self.db.programs.save(program)
        
        #Declare collection for scheduling messages
        self.collection_schedules = self.getCollection(self.db, ("%s_schedules" % program_name))
        #self.collection_schedules.drop()
        #self.collection_schedules.save({"datetime":dFuture.isoformat(),
                                       #"participant-phone": "06",
                                       #"interaction-id": "0",
                                       #"dialogue-id": "0"});

        #Declare collection for loging messages
        self.collection_logs = self.getCollection (self.db, ("%s_logs" % program_name))
        self.collection_logs.drop()
        self.collection_logs.save({"datetime":dPast.isoformat(),
                                   "participant-phone":"06",
                                   "interaction-id": "0",
                                   "dialogue-id": "0"});
                
        #Starting the test
        self.worker.init_program_db(program_name)   
        schedules = self.worker.schedule_participant_dialogue(program['participants'][0], program['dialogues'][0])
        
        self.assertEqual(self.collection_logs.count(), 1);
        self.assertEqual(self.collection_schedules.count(), 1);
        
        
    def test07_schedule_interaction_while_interaction_inschedule(self):
        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes = 30)
        dFuture = datetime.now() + timedelta(minutes = 30)
        
        program_name = "M5H"
        
        #set up of the data
        connection = pymongo.Connection("localhost",27017)
        self.db = connection['test']
        
        program = json.loads(self.simpleProgram)['program']
        self.db.programs.save(program)
        
        #Declare collection for scheduling messages
        self.collection_schedules = self.getCollection(self.db, ("%s_schedules" % program_name))
        self.collection_schedules.drop()
        self.collection_schedules.save({"datetime":dFuture.isoformat(),
                                       "participant-phone": "06",
                                       "interaction-id": "1",
                                       "dialogue-id": "0"});

        #Declare collection for loging messages
        self.collection_logs = self.getCollection (self.db, ("%s_logs" % program_name))
        self.collection_logs.drop()
        self.collection_logs.save({"datetime":dPast.isoformat(),
                                   "participant-phone":"06",
                                   "interaction-id": "0",
                                   "dialogue-id": "0"});
                
        #Starting the test
        self.worker.init_program_db(program_name)   
        schedules = self.worker.schedule_participant_dialogue(program['participants'][0], program['dialogues'][0])
        
        self.assertEqual(self.collection_logs.count(), 1);
        self.assertEqual(self.collection_schedules.count(), 1);
      
    @inlineCallbacks 
    def test08_schedule_interaction_that_has_expired(self):
        dNow = datetime.now()
        dPast = datetime.now() - timedelta(minutes = 40)
        dLaterPast = datetime.now() - timedelta(minutes = 70)
        
        program_name = "M5H"
        
        #Message to be received
        event = Message.from_json(self.controlMessage)
        event['action'] = "resume"
        event['content'] = program_name
        
        #set up of the data
        connection = pymongo.Connection("localhost",27017)
        self.db = connection['test']
        
        self.db.programs.save(Message.from_json(self.simpleProgram)['program'])
        
        #Declare collection for scheduling messages
        self.collection_schedules = self.getCollection(self.db, ("%s_schedules" % program_name))
        self.collection_schedules.save({"datetime":dPast.isoformat(),
                                       "participant-phone": "06",
                                       "interaction-id": "0",
                                       "dialogue-id": "0"});

        #Declare collection for loging messages
        self.collection_logs = self.getCollection (self.db, ("%s_logs" % program_name))
        self.collection_logs.save({"datetime":dLaterPast.isoformat(),
                                       "participant-phone":"06",
                                       "interaction-id": "0",
                                       "dialogue-id": "0"});
                
        #Starting the test
        yield self.send(event,'control')
        
        self.assertEqual(self.collection_logs.count(), 2);
        self.assertEqual(self.collection_schedules.count(), 0);
        
        
  
    def test09_schedule_at_fixed_time(self):
        dNow = datetime.now()
        dFuture = datetime.now() + timedelta(days=2, minutes=30)
        
        program_name = "M5H"
        
        #set up of the data
        connection = pymongo.Connection("localhost",27017)
        self.db = connection['test']
        
        program = json.loads(self.simpleProgram_announcement_fixedtime)['program']
        program['dialogues'][0]['interactions'][0]['year'] = dFuture.year
        program['dialogues'][0]['interactions'][0]['month'] = dFuture.month
        program['dialogues'][0]['interactions'][0]['day'] = dFuture.day
        program['dialogues'][0]['interactions'][0]['hour'] = dFuture.hour
        program['dialogues'][0]['interactions'][0]['minute'] = dFuture.minute        
        self.db.programs.save(program)
        
        #Declare collection for scheduling messages
        self.collection_schedules = self.getCollection(self.db, ("%s_schedules" % program_name))
        self.collection_schedules.drop()
 
        #Declare collection for loging messages
        self.collection_logs = self.getCollection (self.db, ("%s_logs" % program_name))
        self.collection_logs.drop()
                
        #Starting the test
        self.worker.init_program_db(program_name)   
        self.worker.schedule_participant_dialogue(program['participants'][0], program['dialogues'][0])
        
        self.assertEqual(self.collection_schedules.count(), 1)
        schedule = self.collection_schedules.find_one()
        schedule_datetime = datetime.strptime(schedule['datetime'],"%Y-%m-%dT%H:%M:%S")
        self.assertEquals(schedule_datetime.year, dFuture.year)
        self.assertEquals(schedule_datetime.hour, dFuture.hour)
        self.assertEquals(schedule_datetime.minute, dFuture.minute)
        

    #@inlineCallbacks
    #def test10_scheduled_but_tim(self):
        #self.assertTrue(False)
        
    def test11_generate_question(self):
        self.assertTrue(False)
    
    #@inlineCallbacks    
    #def test12_2dialogues_updated_2message_scheduled(self):
        #self.assertTrue(False)
    
    #@inlineCallbacks
    #def test13_resend_failed_message(self):
        ##control from the user
        #self.assertTrue(False)
    
    #@inlineCallbacks    
    #def test14_add_participant_is_scheduling_dialogues(self):
        #self.assertTrue(False)
        
    #@inlineCallbacks
    #def test15_after_reply_Goto_Dialogue(self):
        #self.assertTrue(False)
    
    #@inlineCallbacks
    #def test16_after_reply_send_feedback(self):
        #self.assertTrue(False)
        
    #@inlineCallbacks
    #def test17_restarting_do_not_schedule_or_send_message(self):
        #self.assertTrue(False)