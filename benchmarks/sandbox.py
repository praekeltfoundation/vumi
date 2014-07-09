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


class BenchWorker(JsSandbox):

    WORKER_QUEUE = DeferredQueue()

    @inlineCallbacks
    def startWorker(self):
        yield JsSandbox.startWorker(self)
        self.WORKER_QUEUE.put(self)


@inlineCallbacks
def run_bench(loops=100):
    opts = VumiOptions()
    opts.postOptions()
    worker_creator = WorkerCreator(opts.vumi_options)

    app = worker_creator.create_worker_by_class(JsSandbox, {
        "transport_name": "dummy",
        "javascript": "",
    })

    transport = worker_creator.create_worker_by_class(Transport, {
        "transport_name": "dummy",
    })

    yield app.startService()
    yield transport.startService()
    print "Waiting for worker ..."
    worker = yield BenchWorker.WORKER_QUEUE.get()
    print worker

    start = time.time()
    for i in range(loops):
        transport.publish_message(content="Hi!")
        # TODO: wait for message

    elapsed = time.time() - start
    print elapsed

    yield transport.stopService()
    yield app.stopService()


if __name__ == "__main__":
    log.startLogging(sys.stdout)
    reactor.callLater(0, run_bench, loops=100)
    reactor.run()
