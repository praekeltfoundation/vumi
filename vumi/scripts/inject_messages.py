# -*- test-case-name: vumi.scripts.tests.test_inject_messages -*-
import sys
import json
from twisted.python import usage
from twisted.internet import reactor, task
from twisted.internet.defer import maybeDeferred, Deferred, inlineCallbacks
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
        self.reader = task.LoopingCall(self.readline)
        self.reader.start(0)

    def emit(self, obj):
        if self.config['verbose']:
            sys.stdout.write('%s\n' % obj)

    def readline(self):
        line = sys.stdin.readline()
        if line:
            line = line.strip()
            self.emit(line)
            self.process_line(line)
        else:
            self.reader.stop()
            reactor.stop()

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
    worker_creator.create_worker(
        'vumi.scripts.inject_messages.MessageInjector', options)


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
