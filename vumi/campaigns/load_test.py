# -*- test-case-name: vumi.campaigns.tests.test_load_test -*-

from datetime import datetime

import pytz
from twisted.python import log
from twisted.internet.defer import inlineCallbacks, succeed

from vumi.service import Worker
from vumi.database.base import setup_db, get_db
from vumi.database.message_io import ReceivedMessage
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry


class RollbackTransaction(Exception):
    """
    Bail early from a runInteraction transaction.
    """
    def __init__(self, return_value):
        self.return_value = return_value


class TransactionResponse(object):
    def __init__(self):
        self.replies = []
        self.metrics = []

    def add_reply(self, message_content):
        self.replies.append(message_content)

    def add_metric(self, name, value):
        self.metrics.append((name, value))


class TransactionHandler(object):
    def __call__(self, txn, *args, **kw):
        self.txn = txn
        self.response = TransactionResponse()
        self.handle(*args, **kw)
        return self.response

    def rollback(self):
        raise RollbackTransaction(self.response)

    def add_reply(self, message_content):
        self.response.add_reply(message_content)

    def add_metric(self, name, value):
        self.response.add_metric(name, value)

    def handle(self, *args, **kw):
        raise NotImplementedError()


class CampaignCompetitionHandler(TransactionHandler):
    def validate_unique_code(self, message):
        code = message['message'].strip()
        valid, reason = UniqueCode.burn_code(self.txn, code)
        if not valid:
            self.add_reply("Invalid code: %s" % (reason,))
            self.rollback()
        return code

    def check_limits(self, message):
        count = CampaignEntry.count_entries(self.txn, message['from_msisdn'])
        if count >= 20:
            return False
        since = datetime.utcnow().replace(tzinfo=pytz.UTC, hour=0, minute=0, second=0)
        count = CampaignEntry.count_entries_since(self.txn, since, message['from_msisdn'])
        return count < 5

    def enter_campaign(self, msg_id, message, code, voucher_id):
        CampaignEntry.enter(self.txn, msg_id, code, message['from_msisdn'], voucher_id)

    def vend_voucher(self, message):
        try:
            voucher = VoucherCode.vend_code(self.txn, message.get('network', None))
        except:
            self.add_reply("Valid code. No vouchers, sorry.")
            return None
        self.add_reply("Valid code. Have a voucher: %s" % (voucher.code,))
        return voucher.id

    def handle(self, msg_id, message):
        code = self.validate_unique_code(message)
        if not self.check_limits(message):
            self.add_reply("Valid code, but you're over your limit for the day.")
            return
        voucher_id = self.vend_voucher(message)
        self.enter_campaign(msg_id, message, code, voucher_id)


class CampaignCompetitionWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the CampaignCompetitionWorker with config: %s" % self.config)
        self.setup_db()
        listen_rkey = self.config.get('listen_rkey', 'campaigns.loadtest.incoming')
        yield self.consume(listen_rkey, self.consume_message)

    def setup_db(self):
        try:
            setup_db('loadtest', user='vumi', password='vumi', database='loadtest')
        except:
            log.msg("Unable to create db pool, assuming it already exists.")
        self.db = get_db('loadtest')

    def stopWorker(self):
        log.msg("Stopping the CampaignCompetitionWorker")

    def send_reply(self, source_msg, msg_content):
        pass

    def consume_message(self, message):
        self.process_message(message.payload)

    def run_transaction(self, handler, *args, **kw):
        def _eb(f):
            f.trap(RollbackTransaction)
            return f.value.return_value
        return self.db.runInteraction(handler(), *args, **kw).addErrback(_eb)

    @inlineCallbacks
    def process_message(self, message):
        log.msg("Processing message: %s" % (message))
        msg_id = yield self.db.runInteraction(ReceivedMessage.receive_message, message)
        response = yield self.run_transaction(CampaignCompetitionHandler, msg_id, message)
        for reply in response.replies:
            yield self.send_reply(message, reply)

