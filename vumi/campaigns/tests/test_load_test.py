from datetime import datetime, timedelta

from twisted.internet.defer import DeferredList

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry
from vumi.database.message_io import ReceivedMessage
from vumi.campaigns.load_test import CampaignCompetitionWorker
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

    def consume(self, *args, **kw):
        pass


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

    def test_receive_invalid_message(self):
        """
        If we have an invalid message, bail with an appropriate reply.
        """
        ccw = self.get_ccw()
        msg = self.mkmsg("foo", "s2")
        d = ccw.process_message(msg)

        def _cb(_):
            self.assertEquals([(msg, "Invalid code: invalid")], ccw.replies)
        d.addCallback(_cb)

        def _txn(txn):
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            self.assertEquals(0, CampaignEntry.count_entries(txn))
        d.addCallback(self.ricb, _txn)
        return d

    def test_receive_valid_message_voucher_available(self):
        """
        If we have a valid message, dispense a voucher.
        """
        ccw = self.get_ccw()
        msg = self.mkmsg("aaa", "s2")
        d = ccw.process_message(msg)
        def _cb(_):
            self.assertEquals([(msg, "Valid code. Have a voucher: xyz1")], ccw.replies)
        d.addCallback(_cb)

        def _txn(txn):
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            self.assertEquals(1, CampaignEntry.count_entries(txn))
        d.addCallback(self.ricb, _txn)
        return d

    def test_receive_valid_message_voucher_unavailable(self):
        """
        If we have a valid message but no voucher, apologise.
        """
        ccw = self.get_ccw()
        msg = self.mkmsg("aaa", "s3")
        d = ccw.process_message(msg)
        def _cb(_):
            self.assertEquals([(msg, "Valid code. No vouchers, sorry.")], ccw.replies)
        d.addCallback(_cb)

        def _txn(txn):
            self.assertEquals(1, ReceivedMessage.count_messages(txn))
            self.assertEquals(1, CampaignEntry.count_entries(txn))
        d.addCallback(self.ricb, _txn)
        return d

    def test_over_quota(self):
        """
        If the limit has been reached, don't vend a code.
        """
        ccw = self.get_ccw()
        msgs = [self.mkmsg(c*3, "s3") for c in 'abcde']
        d = DeferredList([ccw.process_message(m) for m in msgs])
        def _cb(_):
            self.assertEquals(["Valid code. No vouchers, sorry."]*5,
                              [r[1] for r in ccw.replies])
            ccw.replies = []
        d.addCallback(_cb)

        def _txn(txn):
            self.assertEquals(5, ReceivedMessage.count_messages(txn))
            self.assertEquals(5, CampaignEntry.count_entries(txn))
        d.addCallback(self.ricb, _txn)

        d.addCallback(lambda _: ccw.process_message(self.mkmsg('fff')))
        def _cb(_):
            self.assertEquals(["Valid code, but you're over your limit for the day."],
                              [r[1] for r in ccw.replies])
        d.addCallback(_cb)

        def _txn(txn):
            self.assertEquals(6, ReceivedMessage.count_messages(txn))
            self.assertEquals(5, CampaignEntry.count_entries(txn))
        d.addCallback(self.ricb, _txn)

        return d
