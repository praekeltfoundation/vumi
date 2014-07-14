# -*- test-case-name: vumi.scripts.tests.test_benchmark_persist -*-
import sys
import time
from twisted.python import usage
from twisted.internet import reactor
from twisted.internet.defer import maybeDeferred, inlineCallbacks, DeferredList

from vumi.message import TransportUserMessage
from vumi.persist.model import Model
from vumi.persist.txriak_manager import TxRiakManager
from vumi.persist.fields import VumiMessage


class Options(usage.Options):
    optParameters = [
        ["messages", "m", "1000",
         "Total number of messages to write and read back."],
        ["concurrent-messages", "c", "100",
         "Number of messages to read and write concurrently"],
    ]

    longdesc = """Benchmarks vumi.persist.model.Model"""


class MessageModel(Model):
    msg = VumiMessage(TransportUserMessage)


class WriteReadBenchmark(object):
    """
    Writes messages to Riak and then reads them back.
    """

    def __init__(self, options):
        self.messages = int(options['messages'])
        self.concurrent = int(options['concurrent-messages'])

    def make_batches(self):
        num_batches, rem = divmod(self.messages, self.concurrent)
        batches = [self.make_batch(i, self.concurrent)
                   for i in range(num_batches)]
        if rem:
            batches.append(self.make_batch(num_batches, rem))
        return batches

    def make_batch(self, batch_no, num_msgs):
        return [TransportUserMessage(to_addr="1234", from_addr="5678",
                    transport_name="bench", transport_type="sms",
                    content="Batch: %d. Msg: %d" % (batch_no, i))
                for i in range(num_msgs)]

    def write_batch(self, model, msgs):
        print "  Writing %d messages." % len(msgs)
        deferreds = []
        for msg in msgs:
            msg_obj = model(key=msg['message_id'], msg=msg)
            deferreds.append(msg_obj.save())
        return DeferredList(deferreds)

    def read_batch(self, model, msgs):
        print "  Reading %d messages." % len(msgs)
        deferreds = []
        for msg in msgs:
            deferreds.append(model.load(msg['message_id']))
        return DeferredList(deferreds)

    @inlineCallbacks
    def run(self):
        manager = TxRiakManager.from_config({'bucket_prefix': 'test.bench.'})
        model = manager.proxy(MessageModel)
        yield manager.purge_all()

        msg_batches = self.make_batches()

        start = time.time()

        for batch in msg_batches:
            yield self.write_batch(model, batch)

        write_done = time.time()
        write_time = write_done - start
        print "Write took %.2f seconds (%.2f msgs/s)" % (
                write_time, self.messages / write_time)

        result_batches = []
        for batch in msg_batches:
            r = yield self.read_batch(model, batch)
            result_batches.append(r)

        read_done = time.time()
        read_time = read_done - write_done
        print "Read took %.2f seconds (%.2f msgs/s)" % (
                read_time, self.messages / read_time)

        for batch, result_batch in zip(msg_batches, result_batches):
            for msg, (good, stored_msg) in zip(batch, result_batch):
                if not good or stored_msg is None:
                    raise RuntimeError("Failed to retrieve message (%s)"
                                       % msg['content'])
                if not(msg == stored_msg.msg):  # TODO: fix message !=
                    raise RuntimeError("Message %r does not equal stored"
                                       " message %r" % (msg, stored_msg.msg))

        print "Messages retrieved successfully."

        yield manager.purge_all()
        print "Messages purged."

if __name__ == '__main__':
    try:
        options = Options()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    bench = WriteReadBenchmark(options)

    def _eb(f):
        f.printTraceback()

    def _main():
        d = maybeDeferred(bench.run)
        d.addErrback(_eb)
        d.addBoth(lambda _: reactor.stop())

    reactor.callLater(0, _main)
    reactor.run()
