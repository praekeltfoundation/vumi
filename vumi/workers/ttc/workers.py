# -*- test-case-name: vumi.workers.ttc.tests.test_ttc -*-

import sys

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.enterprise import adbapi
from twisted.internet import task

#from twistar.dbobject import DBObject
#from twistar.registry import Registry

import pymongo

from datetime import datetime, time, date, timedelta

from vumi.application import ApplicationWorker
from vumi.message import Message, TransportUserMessage
from vumi.application import SessionManager

#from vumi.database.base import setup_db, get_db, close_db, UglyModel

#class ParticipantModel(UglyModel):
    #TABLE_NAME = 'participant_items'
    #fields = (
        #('id', 'SERIAL PRIMARY KEY'),
        #('phone_number','int8 UNIQUE NOT NULL'),
        #)
    #indexes = ('phone_number',)
    
    #@classmethod
    #def get_items(cls, txn):
        #items = cls.run_select(txn,'')
        #if items:
            #items[:] = [cls(txn,*item) for item in items]
            #return items
            ##return cls(txn, *items[0])
        #return None
    
    #@classmethod
    #def create_item(cls, txn, number):
        #params = {'phone_number': number}
        #txn.execute(cls.insert_values_query(**params),params)
        #txn.execute("SELECT lastval()")
        #return txn.fetchone()[0]

#Models#
#CREATE TABLE dialogues (id SERIAL PRIMARY KEY, name VARCHAR(50),type VARCHAR(20)) 
#CREATE TABLE interactions (id SERIAL PRIMARY KEY, name VARCHAR, content VARCHAR(50), schedule_type VARCHAR(30), dialogue_id INT)
#CREATE TABLE schedules (id SERIAL PRIMARY KEY, type VARCHAR(30), interaction_id INT)
#CREATE TABLE participants (id SERIAL PRIMARY KEY, phone INT, name VARCHAR(50))

#Model Relations#
#class Dialogue(DBObject):
    #HASMANY=['interactions']
    
#class Interaction(DBObject):
    #BELONGSTO=['dialogue']

#class Schedule(DBObject):
    #BELONGSTO=['interaction']

#class Participant(DBObject):
    #pass

#class SentMessage(DBObject):
    #HASMANY=['participants']

#Registry.register(Dialogue, Interaction, Participant, SentMessage)

class TtcGenericWorker(ApplicationWorker):
    
    def databaseAccessSuccess(self, result):
        log.msg("Databasee Access Succeed %s" %result)
    
    def databaseAccessFailure(self, failure):
        log.msg("Databasee Access Succeed %s" % failure)
    
    
    @inlineCallbacks
    def startWorker(self):
	log.msg("One Generic Worker is starting")
        super(TtcGenericWorker, self).startWorker()
        self.control_consumer = yield self.consume(
            '%(control_name)s.control' % self.config,
            self.consume_control,
            message_class=Message)
    
        #config
        self.transport_name = self.config['transport_name']
	self.control_name = self.config['control_name']
        self.transport_type = 'sms'

        #some basic local recording
	self.record = []

        # Try to access database with Ugly model
        #self.setup_db = setup_db(ParticipantModel)
        #self.db = setup_db('test', database='test',
        #         user='vumi',
        #         password='vumi',
        #         host='localhost')
        #self.db.runQuery('SELECT 1')
    
        # Try to Access Redis
        #self.redis = SessionManager(db=0, prefix="test")
        
        # Try to Access relational database with twistar
        #Registry.DBPOOL = adbapi.ConnectionPool('psycopg2', "dbname=test host=localhost user=vumi password=vumi")
        #yield Registry.DBPOOL.runQuery("SELECT 1").addCallback(self.databaseAccessSuccess)
    
        # Try to Access Document database with pymongo
        connection = pymongo.Connection("localhost",27017)
        self.db = connection[self.config['database']]
	
	log.msg("Connected to dababase %s" % self.config['database'])
	
	self.sender = None
	self.program_name = None
    
    def consume_user_message(self, message):
        log.msg("User message: %s" % message['content'])
    
    def init_program(self, program):
	log.msg("Initialization of the program")
	self.program_name = program["name"]
	log.msg("Program name:%s"% self.program_name)
	#register program in programs collection
	if (self.db.programs.find_one({"name":self.program_name})):
	    log.msg("Program already exist in database... updating")
	else:
	    self.db.programs.save(program)
	#Declare necessary collection
	collection_schedules_name = ("%s_schedules" % self.program_name)
	if ( collection_schedules_name in self.db.collection_names()):
	    self.collection_schedules = self.db[collection_schedules_name]
	else:
	    self.collection_schedules = self.db.create_collection(collection_schedules_name)
	
	
	collection_logs_name = ("%s_logs" % self.program_name)
	if ( collection_logs_name in self.db.collection_names()):
	    self.collection_logs = self.db[collection_logs_name]
	else:
	    self.collection_logs = self.db.create_collection(collection_logs_name)
	    
    #@inlineCallbacks
    def consume_control(self, message):
        log.msg("Control message!")
        #data = message.payload['data']
        self.record.append(('config',message))
        if (message.get('program')):
            program = message['program']
	    #program_name = program['name']
            #log.msg("Receive a program with name: %s" % program_name)
            #MongoDB#
	    self.init_program(program)
	    if 'participants' in program:
		self.schedule_participants_dialogue(program['participants'], self.get_dialogue(program,"0"))
		
            #Redis#
            #self.redis.create_session("program")
            #self.redis.save_session("program", program)
            #session = self.redis.load_session("program")
            #log.msg("Message stored and retrieved %s" % session.get('name'))
            
            #UglyModel#
            #self.db.runInteraction(ParticipantModel.create_item,68473)        
            #name = program.get('name')
            #group = program.get('group',{})
            #number = group.get('number')
            #log.msg("Control message %s to be add %s" % (number,name))
      
            #Twistar#                
            #def failure(error):
                #log.msg("failure while saving %s" %error)
                
            #if(program.get('dialogues')):
                #yield self.saveDialoguesDB(program.get('dialogues'))
            #if(program.get('participants')):
                #yield self.saveParticipantsDB(program.get('participants'))
        
        elif (message.get('participants')):
            program = self.db.programs.find_one({"name":self.program_name})
	    if 'participants' in program: 
		program['participants'] = program['participants'] + message.get('participants')
	    else:
		program['participants'] = message.get('participants')
            self.db.programs.save(program)
            #self.record.append(('config',message))
            #yield self.saveParticipantsDB(message.get("participants"))

	#start looping process of the scheduler
	if (self.sender == None):
	    self.sender = task.LoopingCall(self.send_scheduled)
	    self.sender.start(30.0)
	
	    
    
    def dispatch_event(self, message):
        log.msg("Event message!")
    
    #TODO: fire error feedback if the dialogue do not exit anymore
    #TODO: if dialogue is deleted, need to remove the scheduled message (or they are also canceled if cannot find the dialogue)
    @inlineCallbacks
    def send_scheduled(self):
        log.msg('Sending Scheduled message start')
	if (self.program_name == None):
	    log.msg("Error scheduling starting but worker is not yet initialized")
	    return
        toSends = self.collection_schedules.find(spec={"datetime":{"$lt":datetime.now().isoformat()}},sort=[("datetime",1)])
        for toSend in toSends:
            self.collection_schedules.remove({"_id":toSend.get('_id')})
            program = self.db.programs.find_one({"name":self.program_name})
	    try:
		interaction = self.get_interaction(program, toSend['dialogue_id'], toSend['interaction_id'])
		log.msg("Send scheduled message %s to %s" % (interaction['content'], toSend['participant_phone'])) 
		message = TransportUserMessage(**{'from_addr':program['shortcode'],
		                            'to_addr':toSend.get('participant_phone'),
		                            'transport_name':self.transport_name,
		                            'transport_type':self.transport_type,
		                            'transport_metadata':'',
		                            'content':interaction['content']
		                            })
		yield self.transport_publisher.publish_message(message);
		self.collection_logs.save({"datetime":datetime.now().isoformat(),"message":message.payload})
	    except:
		log.msg("Error while getting schedule reference, scheduled message dumpted: %s - %s" % (toSend['dialogue_id'],toSend['interaction_id']))
		log.msg("Exception is %s" % sys.exc_info()[0])
    #MongoDB do not support fetching a subpart of an array
    #may not be necessary in the near future
    #https://jira.mongodb.org/browse/SERVER-828
    #https://jira.mongodb.org/browse/SERVER-3089
    def get_interaction(self, program, dialogue_id, interaction_id):
        for dialogue in program['dialogues']:
	    if dialogue["dialogue_id"]== dialogue_id:
		for interaction in dialogue["interactions"]:
		    if interaction["interaction_id"]==interaction_id:
			return interaction
		    
    def get_dialogue(self, program, dialogue_id):
	for dialogue in program['dialogues']:
	    if dialogue["dialogue_id"]== dialogue_id:
		return dialogue

    def schedule_participants_dialogue(self, participants, dialogue):
	for participant in participants:
	    self.schedule_participant_dialogue(participant, dialogue)
		    
    #TODO: manage multiple timezone
    #TODO: manage other schedule type
    #TODO: decide which id should be in an schedule object
    def schedule_participant_dialogue(self, participant, dialogue):
        #schedules = self.collection_schedules
        previousDateTime = None
        schedules = []
        for interaction in dialogue.get('interactions'):
            schedule = {"datetime":None, 
                        "participant_phone": participant.get('phone'), 
                        "dialogue_id":dialogue.get('dialogue_id'), 
                        "interaction_id":interaction.get("interaction_id")}
            if (dialogue.get('type')=="sequential"):
                if (interaction.get('schedule_type')=="immediately"):
                    currentDateTime = datetime.now()
                if (interaction.get('schedule_type')=="wait"):
                    currentDateTime = previdousDateTime + timedelta(minutes=2)
                schedule["datetime"] = currentDateTime.isoformat()
            schedules.append(schedule)
            previdousDateTime = currentDateTime
	    self.collection_schedules.save(schedule)
        return schedules
            #schedules.save(schedule)
    
    #Deprecated
    @inlineCallbacks
    def saveParticipantsDB(self, participants):
        log.msg("received a list of phone")
        for participant in participants:
            oParticipant = yield Participant.find(where=['phone = ?', participant.get('phone')],limit=1)
            if (oParticipant == None):
                oParticipant = yield Participant(phone=participant.get('phone'),
                                           name=participant.get('name')).save()
            else:
                #if (participant.get('name')):
                oParticipant.name = participant.get('name')
                yield oParticipant.save()
    
    #Deprecated
    @inlineCallbacks
    def saveDialoguesDB(self, dialogues):
        for dialogue in dialogues:
            #oDialogue = yield Dialogue(name=dialogue.get('name'),type=dialogue.get('type')).save().addCallback(onDialogueSave,dialogue.get('interactions')).addErrback(failure)
            oDialogue = yield Dialogue.find(where=['name = ?',dialogue.get('name')], limit=1)
            if (oDialogue==None):
                oDialogue = yield Dialogue(name=dialogue.get('name'),type=dialogue.get('type')).save().addErrback(failure)
            else:
                oDialogue.name = dialogue.get('name')
                yield oDialogue.save()
            for interaction in dialogue.get('interactions'):
                if interaction.get('type')== "announcement":
                    oInteraction = yield Interaction.find(where=['name = ?',interaction.get('name')], limit=1)
                    if (oInteraction==None):
                        oInteraction = yield Interaction(content=interaction.get('content'),
                                                         name=interaction.get('name'),
                                                         schedule_type=interaction.get('schedule_type'), 
                                                         dialogue_id=oDialogue.id).save().addErrback(failure)
                    else:
                        oInteraction.content = interaction.get('content')
                        oInteraction.name = interaction.get('name')
                        oInteraction.schedule_type=interaction.get('schedule_type')
                        yield oInteraction.save()
                    #yield Schedule(type=interaction.get("schedule_type"),
                                   #interaction_id=oInteraction.id).save()