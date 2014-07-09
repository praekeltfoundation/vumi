"""
Benchmark sandbox message processing round trip.
"""

import sys
import time

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.python import log

from vumi.application.sandbox import JsSandbox
from vumi.transports.base import Transport
from vumi.service import WorkerCreator
from vumi.servicemaker import VumiOptions


class BenchTransport(Transport):

    WORKER_QUEUE = DeferredQueue()

    @inlineCallbacks
    def startWorker(self):
        yield Transport.startWorker(self)
        self.message_queue = DeferredQueue()
        self.WORKER_QUEUE.put(self)

    def handle_outbound_message(self, msg):
        self.message_queue.put(msg)


class BenchApp(JsSandbox):

    WORKER_QUEUE = DeferredQueue()

    @inlineCallbacks
    def startWorker(self):
        yield JsSandbox.startWorker(self)
        self.WORKER_QUEUE.put(self)

    def sandbox_id_for_message(self, msg_or_event):
        return "DUMMY_SANDBOX_ID"


class Timer(object):
    def __init__(self):
        self.current_time = None
        self.times = []

    def __enter__(self, *args, **kw):
        assert self.current_time is None
        self.current_time = time.time()

    def __exit__(self, *args, **kw):
        assert self.current_time is not None
        self.times.append(time.time() - self.current_time)
        self.current_time = None

    def total(self):
        return sum(self.times)

    def loops(self):
        return len(self.times)

    def mean(self):
        return self.total() / self.loops()


@inlineCallbacks
def run_bench(loops):
    opts = VumiOptions()
    opts.postOptions()
    worker_creator = WorkerCreator(opts.vumi_options)

    app = worker_creator.create_worker_by_class(BenchApp, {
        "transport_name": "dummy",
        "javascript": """
            api.on_inbound_message = function(command) {
                this.request('outbound.reply_to', {
                    content: 'reply',
                    in_reply_to: command.msg.message_id,
                },
                function (reply) {
                    this.done();
                });
            };
        """,
        "sandbox": {
            'outbound': {
                'cls': 'vumi.application.sandbox.OutboundResource',
            },
        },
    })

    transport = worker_creator.create_worker_by_class(BenchTransport, {
        "transport_name": "dummy",
    })

    yield transport.startService()
    log.msg("Waiting for transport ...")
    yield BenchTransport.WORKER_QUEUE.get()

    yield app.startService()
    log.msg("Waiting for worker ...")
    yield BenchApp.WORKER_QUEUE.get()

    print "Starting %d loops ..." % (loops,)
    timer = Timer()
    for i in range(loops):
        with timer:
            transport.publish_message(
                content="Hi!",
                to_addr="+1234",
                from_addr="+5678",
                transport_type="ussd",
            )
            reply = yield transport.message_queue.get()
            log.msg(reply['message_id'])

    print "Total time: %.2f" % timer.total()
    print "Time per message: %g" % timer.mean()
    print "  Loops: %d" % timer.loops()

    yield transport.stopService()
    yield app.stopService()
    reactor.stop()


if __name__ == "__main__":
    # log.startLogging(sys.stdout)
    reactor.callLater(0, run_bench, loops=100)
    reactor.run()
