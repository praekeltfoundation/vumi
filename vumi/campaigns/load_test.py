# -*- test-case-name: vumi.campaigns.tests.test_load_test -*-

from datetime import datetime

import pytz
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.database.message_io import ReceivedMessage
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry
from vumi.database.prospect import Prospect
from vumi.campaigns.base import TransactionHandler, DatabaseWorker
from vumi.campaigns.dispatch import DispatchWorker


class CampaignDispatchWorker(DispatchWorker):

    @inlineCallbacks
    def setup_dispatch(self):
        prospect_rkey = self.get_campaign_rkey('prospect_rkey', 'prospect')
        competition_rkey = self.get_campaign_rkey('competition_rkey', 'competition')
        self.prospect_pub = yield self.publish_to(prospect_rkey)
        self.competition_pub = yield self.publish_to(competition_rkey)

    @inlineCallbacks
    def dispatch_message(self, message):
        # TODO: Better keyword handling
        keyword = message['message'].split()[0].lower()
        if keyword not in ['yes', 'vip', 'stop', 'no']:
            self.publish_msg(self.competition_rkey, message)
        self.publish_msg(self.prospect_rkey, message)


class CampaignCompetitionHandler(TransactionHandler):
    def validate_unique_code(self, message):
        code = message['message'].strip()
        valid, reason = UniqueCode.burn_code(self.txn, code)
        if not valid:
            self.add_reply("Invalid code: %s" % (reason,))
            self.rollback()
        return code

    def check_limits(self, message):
        from_msisdn = message['from_msisdn']
        count = CampaignEntry.count_entries(self.txn, from_msisdn)
        if count >= 20:
            return False
        since = datetime.utcnow().replace(tzinfo=pytz.UTC, hour=0, minute=0, second=0)
        count = CampaignEntry.count_entries_since(self.txn, since, from_msisdn)
        return count < 5

    def enter_campaign(self, msg_id, from_msisdn, code, voucher_id):
        CampaignEntry.enter(self.txn, msg_id, code, from_msisdn, voucher_id)

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
        self.enter_campaign(msg_id, message['from_msisdn'], code, voucher_id)


class CampaignCompetitionWorker(DatabaseWorker):

    @inlineCallbacks
    def setup_worker(self):
        listen_rkey = self.get_campaign_rkey('listen_rkey', 'competition')
        yield self.consume(listen_rkey, self.consume_message)

    def send_reply(self, source_msg, msg_content):
        pass

    @inlineCallbacks
    def process_message(self, message):
        log.msg("Processing message: %s" % (message,))
        response = yield self.run_transaction(CampaignCompetitionHandler,
                                              message['msg_id'], message)
        for reply in response.replies:
            yield self.send_reply(message, reply)


class ProspectPoolHandler(TransactionHandler):
    def handle(self, user_id, keyword, content, msg_id):
        prospect = Prospect.get_prospect(self.txn, user_id)
        if not prospect:
            Prospect.create_prospect(self.txn, user_id)
        if not keyword:
            return
        elif keyword == 'yes':
            self.handle_yes(prospect, content, msg_id)
        elif keyword == 'vip':
            self.handle_vip(prospect, content, msg_id)
        elif keyword in ['no', 'stop']:
            self.handle_stop(prospect, msg_id)

    def handle_yes(self, prospect, content, msg_id):
        prospect.opt_in(self.txn, msg_id, content)

    def handle_vip(self, prospect, content, msg_id):
        prospect.update_name(self.txn, msg_id, content)

    def handle_stop(self, prospect, msg_id):
        prospect.opt_out(self.txn, msg_id)


class ProspectPoolWorker(DatabaseWorker):

    @inlineCallbacks
    def setup_worker(self):
        listen_rkey = self.get_campaign_rkey('listen_rkey', 'prospect')
        yield self.consume(listen_rkey, self.consume_message)

    def parse_message(self, message):
        # TODO: Make this sane.
        msg_id = message['msg_id']
        keyword, content = (message['message'].split(None, 1) + [''])[:2]
        user_id = message['from_msisdn']
        return user_id, keyword.lower(), content, msg_id

    def process_message(self, message):
        log.msg("Processing message: %s" % (message))
        user_id, keyword, content, msg_id = self.parse_message(message)
        return self.run_transaction(ProspectPoolHandler, user_id, keyword,
                                    message, msg_id)
