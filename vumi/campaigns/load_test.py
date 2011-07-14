
from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker
from vumi.database.base import setup_db, close_db
from vumi.database.message_io import ReceivedMessage
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry



class CampaignWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the CampaignWorker with config: %s" % self.config)
        self.db = setup_db('loadtest', user='vumi', password='vumi', database='loadtest')
        listen_rkey = self.config.get('listen_rkey', 'campaigns.loadtest.incoming')
        yield self.consume(listen_rkey, self.consume_message)

    def stopWorker(self):
        log.msg("Stopping the CampaignWorker")
        close_db('loadtest')

    def consume_message(self, message):
        log.msg("Processing message: %s" % (message.payload))
