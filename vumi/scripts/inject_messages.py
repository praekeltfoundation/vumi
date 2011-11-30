# -*- test-case-name: vumi.scripts.tests.test_inject_messages -*-
import sys
import json
from twisted.python import usage
from twisted.internet import reactor, threads
from twisted.internet.defer import maybeDeferred, inlineCallbacks
from vumi.message import TransportUserMessage
from vumi.service import Options, Worker, WorkerCreator


class InjectorOptions(Options):
    optParameters = Options.optParameters + [
        ["transport-name", None, None,
            "Name of the transport to inject messages from"],
        ["verbose", "v", False, "Output the JSON being injected"],
    ]

    def postOptions(self):
        if not self['transport-name']:
            raise usage.UsageError("Please provide the "
                                    "transport-name parameter.")


class MessageInjector(Worker):

    @inlineCallbacks
    def startWorker(self):
        self.transport_name = self.config['transport-name']
        self.publisher = yield self.publish_to('%s.inbound' %
                                                self.transport_name)
        self.publisher.require_bind = False

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
            TransportUserMessage(**data))


def main(options):
    vumi_options = {}
    for opt in [i[0] for i in Options.optParameters]:
        vumi_options[opt] = options.pop(opt)

    worker_creator = WorkerCreator(vumi_options)
    worker = worker_creator.create_worker(
        'vumi.scripts.inject_messages.MessageInjector', options)

    in_file = sys.stdin
    out_file = sys.stdout if vumi_options['verbose'] else None

    return worker.process_file(in_file, out_file)


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
