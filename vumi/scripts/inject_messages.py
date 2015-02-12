# -*- test-case-name: vumi.scripts.tests.test_inject_messages -*-
import sys
import json
from twisted.python import usage
from twisted.internet import reactor, threads
from twisted.internet.defer import (maybeDeferred, DeferredQueue,
                                    inlineCallbacks)
from vumi.message import TransportUserMessage
from vumi.service import Worker, WorkerCreator
from vumi.servicemaker import VumiOptions
from vumi.utils import to_kwargs


class InjectorOptions(VumiOptions):
    optParameters = [
        ["transport-name", None, None,
            "Name of the transport to inject messages from"],
        ["direction", None, "inbound",
            "Direction messages are to be sent to."],
        ["verbose", "v", False, "Output the JSON being injected"],
    ]

    def postOptions(self):
        VumiOptions.postOptions(self)
        if not self['transport-name']:
            raise usage.UsageError("Please provide the "
                                    "transport-name parameter.")


class MessageInjector(Worker):

    WORKER_QUEUE = DeferredQueue()

    @inlineCallbacks
    def startWorker(self):
        self.transport_name = self.config['transport-name']
        self.direction = self.config['direction']
        self.publisher = yield self.publish_to(
            '%s.%s' % (self.transport_name, self.direction))
        self.WORKER_QUEUE.put(self)

    def process_file(self, in_file, out_file=None):
        return threads.deferToThread(self._process_file_in_thread,
                                     in_file, out_file)

    def _process_file_in_thread(self, in_file, out_file):
        for line in in_file:
            line = line.strip()
            self.emit(out_file, line)
            threads.blockingCallFromThread(reactor, self.process_line, line)

    def emit(self, out_file, obj):
        if out_file is not None:
            out_file.write('%s\n' % (obj,))

    def process_line(self, line):
        data = {
            'transport_name': self.transport_name,
            'transport_metadata': {},
        }
        data.update(json.loads(line))
        self.publisher.publish_message(
            TransportUserMessage(**to_kwargs(data)))


@inlineCallbacks
def main(options):
    verbose = options['verbose']

    worker_creator = WorkerCreator(options.vumi_options)
    service = worker_creator.create_worker_by_class(
        MessageInjector, options)
    yield service.startService()

    in_file = sys.stdin
    out_file = sys.stdout if verbose else None

    worker = yield MessageInjector.WORKER_QUEUE.get()
    yield worker.process_file(in_file, out_file)
    reactor.stop()


if __name__ == '__main__':
    try:
        options = InjectorOptions()
        options.parseOptions()
    except usage.UsageError, errortext:
        print '%s: %s' % (sys.argv[0], errortext)
        print '%s: Try --help for usage details.' % (sys.argv[0])
        sys.exit(1)

    def _eb(f):
        f.printTraceback()

    def _main():
        maybeDeferred(main, options).addErrback(_eb)

    reactor.callLater(0, _main)
    reactor.run()
