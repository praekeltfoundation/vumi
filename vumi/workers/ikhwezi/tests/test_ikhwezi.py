import yaml

from twisted.python import log
from twisted.trial.unittest import TestCase
from twisted.internet.defer import succeed

from vumi.transports.httprpc.vodacom_messaging import VodacomMessagingResponse
from vumi.workers.ikhwezi.ikhwezi import (
        TRANSLATIONS, QUIZ, IkhweziQuiz, IkhweziQuizWorker, IkhweziModel)
from vumi.tests.utils import FakeRedis
from vumi.database.base import (setup_db, get_db, close_db, UglyModel,
                                TableNamePrefixFormatter)


class IkhweziBaseTest(TestCase):

    def ri(self, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    def _sdb(self, dbname, **kw):
        self._dbname = dbname
        try:
            get_db(dbname)
            close_db(dbname)
        except:
            pass
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


class IkhweziModelTest(IkhweziBaseTest):

    def setUp(self):
        return self.setup_db(IkhweziModel)

    def tearDown(self):
        return self.shutdown_db()

    def test_setup_and_teardown(self):
        self.assertTrue(True)

    def test_insert_and_get_msisdn(self):
        def _txn(txn):
            self.assertEqual(0, IkhweziModel.count_rows(txn))
            IkhweziModel.create_item(txn, msisdn='555', provider='test_provider')
            self.assertEqual(1, IkhweziModel.count_rows(txn))
            item = IkhweziModel.get_item(txn, '555')
            self.assertEqual('555', item.msisdn)
            self.assertEqual('test_provider', item.provider)
            self.assertNotEqual('test', item.attempts)
        d = self.ri(_txn)
        return d

    def test_insert_update_and_get_msisdn(self):
        def _txn(txn):
            self.assertEqual(0, IkhweziModel.count_rows(txn))
            IkhweziModel.create_item(txn, msisdn='555', provider='test_provider')
            self.assertEqual(1, IkhweziModel.count_rows(txn))
            IkhweziModel.update_item(txn, msisdn='555', provider='other', demographic1=1)
            item = IkhweziModel.get_item(txn, '555')
            self.assertEqual('555', item.msisdn)
            self.assertEqual('other', item.provider)
            self.assertEqual(1, item.demographic1)
            self.assertNotEqual('test', item.attempts)
        d = self.ri(_txn)
        return d

class IkhweziQuizTest(IkhweziBaseTest):

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
        self.exit_text = self.quiz['exit']['headertext']
        self.completed_text = self.quiz['completed']['headertext']
        self.config = {
                'web_host': 'vumi.p.org',
                'web_path': '/api/v1/ussd/vmes/'}
        self.setup_db(IkhweziModel)

    def tearDown(self):
        return self.shutdown_db()

    def quiz_respond(self, session_event, request, response_callback):
        ik = IkhweziQuiz(
                self.config,
                self.quiz,
                self.translations,
                self.db)
        return ik.respond(
                self.msisdn,
                session_event,
                self.provider,
                request,
                response_callback)

    def set_exit_text(self, fun):
        self.exit_text = fun(self.quiz['exit']['headertext'])

    def set_completed_text(self, fun):
        self.completed_text = fun(self.quiz['completed']['headertext'])

    def test_answer_out_of_range_1(self):
        """
        The 66 is impossible
        """
        inputs = [1, 66, 1, 1, 1, 1]

        def response_callback(resp):
            pass
        def finish_callback(resp):
            return self.assertTrue(resp.headertext.endswith(self.exit_text))
        self.quiz_respond('new', '*120*112233#', response_callback)
        for i in inputs:
            self.quiz_respond('resume', i, response_callback)
        d = self.quiz_respond('resume', 2, finish_callback)
        return d

    #def test_answer_out_of_range_2(self):
        #"""
        #The 22 is impossible
        #"""
        #inputs = [22, 1, 2, 2, 2, 2]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)

    #def test_answer_wrong_type_1(self):
        #inputs = [1, "is there a 3rd option?", 1, 1, 1, 1]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)


    #def test_answer_wrong_type_2(self):
        #inputs = [1, '*120*11223344#', 1, 1, 1, 2]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)

    #def test_answer_wrong_type_3(self):
        #"""
        #Mismatches on Continue question auto continue
        #"""
        #inputs = [1, 1, "huh", 1, 2, 1, 'exit', 1]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)

    #def test_sequence_1(self):
        #inputs = ['blah', 55, '1', 1, 55, 55, 'blah',
                #'1', 1, 55, 'blah', 55, '1', 2, 2]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)

    #def test_sequence_2(self):
        #inputs = [1, '1', 55, 2, '1', 55, 1, '1',
                #55, 2, 55, '2', 1, '1', 1, '1', 1, 55, '2',
                #55, '1', 1, '1', 55, '2', 'bye', 'no', 1]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)

    #def test_sequence_3(self):
        #inputs = ['demo', '1', 55, 'demo', 55, 55, 55, 55,
                #'2', 55, 'demo', '2', 55, 55, 'demo', '2', 1, 55,
                #55, '1', 55, '2', 55, '2', 55, '2', 55, '2']

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.exit_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('resume', 2, finish_callback)

    #def test_sequence_4(self):
        #"""
        #Check the message on resuming after completion
        #"""
        #inputs = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                #1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

        #def response_callback(resp):
            #pass
        #def finish_callback(resp):
            #self.assertTrue(resp.headertext.endswith(self.completed_text))
        #self.quiz_respond('new', '*120*112233#', response_callback)
        #for i in inputs:
            #self.quiz_respond('resume', i, response_callback)
        #self.quiz_respond('new', '*120:112233#', finish_callback)

    #def test_with_resumes(self):
        #def callback1(resp):
            #self.assertEqual(4, len(resp.option_list))
        #self.quiz_respond('new','*120*112233#', callback1)
        #def callback2(resp):
            #self.assertEqual(2, len(resp.option_list))
        #self.quiz_respond('resume', 1, callback2)
        #def callback3(resp):
            #self.assertEqual(9, len(resp.option_list))
        #self.quiz_respond('resume', 1, callback3)
        #def callback4(resp):
            #self.assertEqual(4, len(resp.option_list))
        #self.quiz_respond('resume', 1, callback4)

        ### simulate resume
        #def callback5(resp):
            #self.assertEqual(4, len(resp.option_list))
        #self.quiz_respond('new', '*120*112233#', callback5)

        ### and 3 more attempts
        #def callback5(resp):
            #self.assertEqual(4, len(resp.option_list))
        #self.quiz_respond('new', '*120*112233#', callback5)
        #def callback5(resp):
            #self.assertEqual(4, len(resp.option_list))
        #self.quiz_respond('new', '*120*112233#', callback5)
        #def callback5(resp):
            #self.assertEqual(0, len(resp.option_list))
        #self.quiz_respond('new', '*120*112233#', callback5)
