from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, maybeDeferred
from twisted.python import usage

from vumi.database.base import setup_db
from vumi.database.message_io import ReceivedMessage
from vumi.database.unique_code import UniqueCode, VoucherCode, CampaignEntry


class Options(usage.Options):
    optFlags = [
        ["create-tables", "c", "Create tables"],
        ["delete-tables", "d", "Delete tables before creating them"],
        ]
    optParameters = [
        ["unique-codes", "u", None, "CSV file containing unique codes"],
        ["voucher-codes", "v", None, "CSV file containing voucher codes"],
        ]


@inlineCallbacks
def delete_tables(db):
    print "Deleting tables..."
    yield CampaignEntry.drop_table(db)
    yield VoucherCode.drop_table(db)
    yield UniqueCode.drop_table(db)
    yield ReceivedMessage.drop_table(db)


@inlineCallbacks
def create_tables(db):
    print "Creating tables..."
    @inlineCallbacks
    def create_if_not_exists(table):
        try:
            yield table.create_table(db)
        except:
            print "'%s' already exists, assuming it's correct..." % (table.__name__,)

    yield create_if_not_exists(ReceivedMessage)
    yield create_if_not_exists(UniqueCode)
    yield create_if_not_exists(VoucherCode)
    yield create_if_not_exists(CampaignEntry)


def filter_lines(filename):
    for line in open(filename):
        line = line.strip()
        if not line:
            continue
        yield line


def parse_vcode_pair(line):
    code, supplier = line.split(',')
    return code.strip(), supplier.strip()


def load_unique_codes(txn, filename):
    print "Loading unique codes..."
    codes = filter_lines(filename)
    return UniqueCode.load_codes(txn, codes)


def load_voucher_codes(txn, filename):
    print "Loading voucher codes..."
    codes = (parse_vcode_pair(line) for line in filter_lines(filename))
    return VoucherCode.load_supplier_codes(txn, codes)


@inlineCallbacks
def main(argv):
    options = Options()
    options.parseOptions(argv)

    db = setup_db('loadtest', user='vumi', password='vumi', database='loadtest')

    if options['delete-tables']:
        yield delete_tables(db)
    if options['create-tables']:
        yield create_tables(db)
    ucodes = options['unique-codes']
    if ucodes:
        yield db.runInteraction(load_unique_codes, ucodes)
    vcodes = options['voucher-codes']
    if vcodes:
        yield db.runInteraction(load_voucher_codes, vcodes)



if __name__ == '__main__':
    import sys

    def _main():
        maybeDeferred(main, sys.argv[1:]).addCallback(lambda _: reactor.stop())
    reactor.callLater(0, _main)
    reactor.run()

