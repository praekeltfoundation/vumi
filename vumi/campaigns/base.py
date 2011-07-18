
from twisted.python import log

from vumi.service import Worker
from vumi.database.base import setup_db, get_db


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


class DatabaseWorker(Worker):

    def startWorker(self):
        log.msg("Starting DatabaseWorker '%s' with config: %s" % (type(self).__name__, self.config))
        self.setup_db()
        return self.setup_worker()

    def setup_worker(self):
        raise NotImplementedError()

    def setup_db(self):
        dbname = self.config.get('dbname', 'loadtest')
        dbuser = self.config.get('dbuser', 'vumi')
        dbpass = self.config.get('dbpass', 'vumi')
        try:
            setup_db(dbname, user=dbuser, password=dbpass, database=dbname)
        except:
            log.msg("Unable to create db pool, assuming it already exists.")
        self.db = get_db(dbname)

    def stopWorker(self):
        log.msg("Stopping DatabaseWorker '%s'" % (type(self).__name__,))

    def consume_message(self, message):
        self.process_message(message.payload)

    def process_message(self, message):
        raise NotImplementedError()
