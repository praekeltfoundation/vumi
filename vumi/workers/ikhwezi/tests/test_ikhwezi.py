import yaml
import random
import redis

from twisted.python import log
from twisted.trial.unittest import TestCase
from twisted.internet.defer import succeed

from vumi.transports.httprpc.vodacom_messaging import VodacomMessagingResponse
from vumi.workers.ikhwezi.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker, IkhweziModel)
from vumi.tests.utils import FakeRedis
from vumi.database.base import (setup_db, get_db, close_db, UglyModel,
                                TableNamePrefixFormatter)


#trans = yaml.load(TRANSLATIONS)
#for t in trans:
    #print ''
    #for k, v in t.items():
        #print k.ljust(12,'.'), v

#translations = {'Zulu': {}, 'Sotho': {}, 'Afrikaans': {}}
#for t in trans:
    #translations['Zulu'][t['English']] = t['Zulu']
    #translations['Sotho'][t['English']] = t['Sotho']
    #translations['Afrikaans'][t['English']] = t['Afrikaans']

#for k,v in translations.items():
    #print ''
    #print k
    #for kk,vv in v.items():
        #print '--->', kk
        #print ' L->', vv

class IkhweziModelBaseTest(TestCase):

    def ri(self, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def _sdb(self, dbname, **kw):
        self._dbname = dbname
        try:
            get_db(dbname)
            close_db(dbname)
        except:
            pass
        # TODO: Pull this out into some kind of config?
        self.db = setup_db(dbname, database=dbname,
                host='localhost',
                user=kw.get('dbuser', 'vumi'),
                password=kw.get('dbpassword', 'vumi'))
        return self.db.runQuery("SELECT 1")

    def setup_db(self, *tables, **kw):
        dbname = kw.pop('dbname', 'test')
        self._test_tables = tables

        def _eb(f):
            raise SkipTest("Unable to connect to test database: %s" % (
                    f.getErrorMessage(),))
        d = self._sdb(dbname)
        d.addErrback(_eb)

        def add_callback(func, *args, **kw):
            # This function exists to create a closure around a
            # variable in the correct scope. If we just add the
            # callback directly in the loops below, we only get the
            # final value of "table", not each intermediate value.
            d.addCallback(lambda _: func(self.db, *args, **kw))
        for table in reversed(tables):
            add_callback(table.drop_table, cascade=True)
        for table in tables:
            add_callback(table.create_table)
        return d

    def shutdown_db(self):
        d = succeed(None)
        for tbl in reversed(self._test_tables):
            d.addCallback(lambda _: tbl.drop_table(self.db))

        def _cb(_):
            close_db(self._dbname)
            self.db = None
        return d.addCallback(_cb)
    pass

class IkhweziModelTest(IkhweziModelBaseTest):

    def setUp(self):
        return self.setup_db(IkhweziModel)

    def tearDown(self):
        return self.shutdown_db()

    #def test_setup_and_teardown(self):
        #self.assertTrue(True)

    #def test_insert_and_get_msisdn(self):
        #def _txn(txn):
            #self.assertEqual(0, IkhweziModel.count_rows(txn))
            #IkhweziModel.create_item(txn, '555', provider='test_provider')
            #self.assertEqual(1, IkhweziModel.count_rows(txn))
            #item = IkhweziModel.get_item(txn, '555')
            #self.assertEqual('555', item.msisdn)
            #self.assertEqual('test_provider', item.provider)
            #self.assertNotEqual('test', item.attempts)
        #d = self.ri(_txn)
        #return d

    def test_insert_update_and_get_msisdn(self):
        def _txn(txn):
            self.assertEqual(0, IkhweziModel.count_rows(txn))
            IkhweziModel.create_item(txn, '555', provider='test_provider')
            self.assertEqual(1, IkhweziModel.count_rows(txn))
            IkhweziModel.update_item(txn, msisdn='555', provider='other', demographic1=1)
            item = IkhweziModel.get_item(txn, '555')
            self.assertEqual('555', item.msisdn)
            self.assertEqual('other', item.provider)
            self.assertEqual(1, item.demographic1)
            self.assertNotEqual('test', item.attempts)
        d = self.ri(_txn)
        return d

class RedisInputSequenceTest(TestCase):

    def setUp(self):
        self.msisdn = '0821234567'
        self.session_event = 'new'
        self.provider = 'test'
        self.quiz = yaml.load(QUIZ)
        trans = yaml.load(TRANSLATIONS)
        self.translations = {'Zulu': {}, 'Sotho': {}, 'Afrikaans': {}}
        for t in trans:
            self.translations['Zulu'][t['English']] = t['Zulu']
            self.translations['Sotho'][t['English']] = t['Sotho']
            self.translations['Afrikaans'][t['English']] = t['Afrikaans']
        self.language = 'English'
        self.exit_text = None
        self.completed_text = None
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}
        self.setup_db()

    def get_quiz_entry(self):
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.db,
                self.msisdn,
                self.session_event,
                self.provider)
        return ik

    def tearDown(self):
        keys = self.ds.keys("vumi_vodacom_ikhwezi*")
        if len(keys) and type(keys) is str:
            keys = keys.split(' ')
        if len(keys):
            for k in keys:
                #print self.ds.get(k)
                self.ds.delete(k)

    def set_ds(self):
        self.ds = redis.Redis("localhost", db=7)

    def setup_db(self):
        dbname = 'ikhwezi'
        try:
            get_db(dbname)
            close_db(dbname)
        except:
            pass
        self.db = setup_db(dbname, database=dbname,
                host='localhost',
                user='vumi',
                password='vumi')
        return self.db.runQuery("SELECT 1")


    def set_exit_text(self, fun):
        self.exit_text = fun(self.quiz['exit']['headertext'])

    def set_completed_text(self, fun):
        self.completed_text = fun(self.quiz['completed']['headertext'])

    def run_input_sequence(self, inputs):
        user_in = inputs.pop(0)
        #print ''
        #print self.session_event
        #print user_in
        ik = self.get_quiz_entry()
        self.session_event = 'resume'
        #print self.session_event
        resp = ik.formulate_response(user_in)
        self.language = ik.language
        self.set_completed_text(ik._)
        self.set_exit_text(ik._)
        #print resp
        if len(inputs):
            return self.run_input_sequence(inputs)
        return resp

    def finish_input_sequence(self, inputs):
        resp = self.run_input_sequence(inputs)
        final_headertext = resp.headertext
        ref_text = self.exit_text
        self.assertTrue(final_headertext.endswith(ref_text))

    def completed_input_sequence(self, inputs):
        resp = self.run_input_sequence(inputs)
        final_headertext = resp.headertext
        ref_text = self.completed_text
        self.assertTrue(final_headertext.endswith(ref_text))

    def test_answer_out_of_range_1(self):
        """
        The 66 is impossible
        """
        inputs = ['*120*112233#', 1, 66, 1, 1, 1, 1, 2]
        self.finish_input_sequence(inputs)

    #def test_answer_out_of_range_2(self):
        #"""
        #The 22 is impossible
        #"""
        #inputs = ['*120*112233#', 22, 2, 2, 2, 2, 2, 2]
        #self.finish_input_sequence(inputs)

    #def test_answer_wrong_type_1(self):
        #inputs = ['*120*112233#', 1, "is there a 3rd option?", 1, 1, 1, 1, 2]
        #self.finish_input_sequence(inputs)

    #def test_answer_wrong_type_2(self):
        #inputs = ['*120*112233#', 1, '*120*11223344#', 1, 1, 1, 2, 2]
        #self.finish_input_sequence(inputs)

    #def test_answer_wrong_type_3(self):
        #"""
        #Mismatches on Continue question auto continue
        #"""
        #inputs = ['*120*112233#', 1, 1, "huh", 1, 2, 1, 'exit', 1, 2]
        #self.finish_input_sequence(inputs)

    #def test_sequence_1(self):
        #inputs = ['*120*112233#', 'blah', 55, '1', 1, 55, 55, 'blah',
                #'1', 1, 55, 'blah', 55, '1', 2, 2, 2]
        #self.finish_input_sequence(inputs)

    #def test_sequence_2(self):
        #"""
        #This sequence answers all questions,
        #forcing an exit response to the final continure question
        #"""
        #inputs = ['*120*112233#', 1, '1', 55, 2, '1', 55, 1, '1',
                #55, 2, 55, '2', 1, '1', 1, '1', 1, 55, '2',
                #55, '1', 1, '1', 55, '2', 'bye', 'no', 1, 2]
        #self.finish_input_sequence(inputs)

    #def test_sequence_3(self):
        #inputs = ['*120*112233#', 'demo', '2', 55, 'demo', 55, 55, 55, 55,
                #'2', 55, 'demo', '2', 55, 55, 'demo', '2', 1, 55,
                #55, '1', 55, '2', 55, '2', 55, '2', 55, '2', 2]
        #self.finish_input_sequence(inputs)

    #def test_sequence_4(self):
        #"""
        #Check the message on resuming after completion
        #"""
        #inputs = ['*120*112233#', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                #1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        #self.finish_input_sequence(inputs)
        #self.session_event = 'new'
        #inputs = ['*120*112233#']
        #self.completed_input_sequence(inputs)

    #def test_with_resumes(self):
        #inputs = ['*120*112233#']
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(4, len(resp.option_list))
        #inputs = [1]
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [1]
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(9, len(resp.option_list))
        #inputs = [1]
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(4, len(resp.option_list))

        ## simulate resume
        #inputs = ['*120*112233#']
        #self.session_event = 'new'
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(4, len(resp.option_list))

        ## and 3 more attempts
        #inputs = [1]
        #self.session_event = 'new'
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [1]
        #self.session_event = 'new'
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(2, len(resp.option_list))
        #inputs = [None]
        #self.session_event = 'new'
        #resp = self.run_input_sequence(inputs)
        #self.assertEqual(0, len(resp.option_list))


#class FakeRedisInputSequenceTest(RedisInputSequenceTest):

    #def tearDown(self):
        #pass

    #def set_ds(self):
        #self.ds = FakeRedis()
