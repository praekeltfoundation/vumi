from datetime import datetime, timedelta
import uuid

from twisted.internet.defer import DeferredList, inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry
from vumi.database.message_io import ReceivedMessage
from vumi.campaigns.load_test import CampaignCompetitionWorker, CampaignDispatchWorker
from vumi.service import WorkerCreator


# TODO: Generalise this?
class WorkerTestCase(UglyModelTestCase):
    def create_worker(self, worker, config):
        global_options = {
            "hostname": "localhost",
            "port": 5672,
            "username": "vumitest",
            "password": "vumitest",
            "vhost": "/test",
            "specfile": "config/amqp-spec-0-8.xml",
            }
        f = []
        class NoQueueWorkerCreator(WorkerCreator):
            def _connect(self, factory, *_args, **_kw):
                f.append(factory)

        creator = NoQueueWorkerCreator(global_options)
        worker_class = "%s.%s" % (worker.__module__,
                                  worker.__name__)
        creator.create_worker(worker_class, config)
        f = f[0]
        return f.buildProtocol(None)


class StubbedCCW(CampaignCompetitionWorker):
    def send_reply(self, source_msg, msg_content):
        self.replies.append((source_msg, msg_content))

    def consume(self, rkey, *args, **kw):
        return rkey

    def publish_to(self, rkey, *args, **kw):
        return rkey


class CampaignCompetitionWorkerTestCase(WorkerTestCase):
    def setUp(self):
        d = self.setup_db(ReceivedMessage, UniqueCode, VoucherCode, CampaignEntry,
                          dbname='loadtest')
        d.addCallback(self.ricb, UniqueCode.load_codes, [c*3 for c in 'abcdefghij'])
        d.addCallback(self.ricb, VoucherCode.load_supplier_codes,
                      [('abc1', 's1'), ('abc2', 's1'), ('xyz1', 's2')])
        return d

    def tearDown(self):
        return self.shutdown_db()

    def get_ccw(self, **kw):
        ccw = self.create_worker(StubbedCCW, kw)
        ccw.replies = []
        ccw.startWorker()
        return ccw

    def mkmsg(self, content, network=None):
        msg = {
            'from_msisdn': '27831234567',
            'to_msisdn': '90210',
            'message': content,
            }
        if network:
            msg['network'] = network
        return msg

    def mkrmsg(self, content, network=None):
        msg = self.mkmsg(content, network)
        msg['transport_message_id'] = uuid.uuid4().get_hex()[10:]
        d = self.ri(ReceivedMessage.receive_message, msg)
        def _cb(msg_id):
            msg['msg_id'] = msg_id
            return msg
        return d.addCallback(_cb)

    @inlineCallbacks
    def test_receive_invalid_message(self):
        """
        If we have an invalid message, bail with an appropriate reply.
        """
        ccw = self.get_ccw()
        msg = yield self.mkrmsg("foo", "s2")
        yield ccw.process_message(msg)

        self.assertEquals([(msg, "Invalid code: invalid")], ccw.replies)

        def _txn(txn):
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            self.assertEquals(0, CampaignEntry.count_entries(txn))

        yield self.ri(_txn)

    @inlineCallbacks
    def test_receive_valid_message_voucher_available(self):
        """
        If we have a valid message, dispense a voucher.
        """
        ccw = self.get_ccw()
        msg = yield self.mkrmsg("aaa", "s2")
        yield ccw.process_message(msg)

        self.assertEquals([(msg, "Valid code. Have a voucher: xyz1")], ccw.replies)

        def _txn(txn):
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            self.assertEquals(1, CampaignEntry.count_entries(txn))

        yield self.ri(_txn)

    @inlineCallbacks
    def test_receive_valid_message_voucher_unavailable(self):
        """
        If we have a valid message but no voucher, apologise.
        """
        ccw = self.get_ccw()
        msg = yield self.mkrmsg("aaa", "s3")
        yield ccw.process_message(msg)

        self.assertEquals([(msg, "Valid code. No vouchers, sorry.")], ccw.replies)

        def _txn(txn):
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            self.assertEquals(1, CampaignEntry.count_entries(txn))

        yield self.ri(_txn)

    @inlineCallbacks
    def test_over_quota(self):
        """
        If the limit has been reached, don't vend a code.
        """
        ccw = self.get_ccw()
        msgs = yield DeferredList([self.mkrmsg(c*3, "s3") for c in 'abcde'])
        yield DeferredList([ccw.process_message(m) for _, m in msgs])
        self.assertEquals(["Valid code. No vouchers, sorry."]*5,
                          [r for _, r in ccw.replies])
        ccw.replies = []

        def _txn(txn):
            self.assertEquals(5, ReceivedMessage.count_messages(txn))
            self.assertEquals(5, CampaignEntry.count_entries(txn))
        yield self.ri(_txn)

        msg = yield self.mkrmsg('fff')
        yield ccw.process_message(msg)

        self.assertEquals(["Valid code, but you're over your limit for the day."],
                          [r for _, r in ccw.replies])

        def _txn(txn):
            self.assertEquals(6, ReceivedMessage.count_messages(txn))
            self.assertEquals(5, CampaignEntry.count_entries(txn))
        yield self.ri(_txn)


class StubbedCDW(CampaignDispatchWorker):
    def dispatch_message(self, message):
        self.dispatched.append(message)

    def setup_dispatch(self):
        pass

    def consume(self, rkey, *args, **kw):
        return rkey

    def publish_to(self, rkey, *args, **kw):
        return rkey

    def send_sms(self, message):
        self.sent_messages.append(message)


class CampaignDispatchWorkerTestCase(WorkerTestCase):
    def setUp(self):
        return self.setup_db(dbname='loadtest')

    def tearDown(self):
        return self.shutdown_db()

    def get_cdw(self, **kw):
        cdw = self.create_worker(StubbedCDW, kw)
        cdw.dispatched = []
        cdw.sent_messages = []
        cdw.startWorker()
        return cdw
