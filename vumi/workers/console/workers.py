from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker
from vumi.message import Message
from vumi.workers.loader import WorkerLoaderBase


class TelnetConsoleWorker(Worker):
    name = None

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting console worker.")
        self.publisher = yield self.publish_to('console.outbound')
        self.consume('console.inbound', self.consume_message, 'console.inbound.%s' % (self.name,))
        log.msg("Started service")

    def consume_message(self, message):
        log.msg("Consumed message %s" % message)
        data = self.process_message(message.payload['message'])
        if data:
            self.publisher.publish_message(Message(recipient=self.name, message=data))

    def process_message(self, data):
        return None

    def stopWorker(self):
        log.msg("Stopping console worker.")


class TelnetConsoleEcho(TelnetConsoleWorker):
    name = 'echo'

    def process_message(self, data):
        return data


class TelnetConsoleReverse(TelnetConsoleWorker):
    name = 'reverse'

    def process_message(self, data):
        return data[::-1]


class TelnetConsoleCounts(TelnetConsoleWorker):
    name = 'counts'

    def process_message(self, data):
        response = []
        if self.config.get('words', False):
            words = len(data.split())
            response.append("%s word%s" % (words, "s" * (words != 1)))
        if self.config.get('chars', False):
            chars = len(data)
            response.append("%s char%s" % (chars, "s" * (chars != 1)))
        return ', '.join(response)



def safe_split(s, sep, maxsplit):
    split = s.split(sep, maxsplit)
    return (split + ['']*(maxsplit+1))[:maxsplit+1]


class TelnetConsoleWorkerManager(WorkerLoaderBase):
    name = 'loaderthing'

    command_prefix = '!'

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting console worker.")

        self.command_prefix = self.config.get('command_prefix', self.command_prefix)

        self.publisher = yield self.publish_to('console.outbound')
        self.consume('console.inbound', self.consume_message, 'console.inbound.%s' % (self.name,))
        log.msg("Started service")

    def consume_message(self, message):
        log.msg("Consumed message %s" % message)
        msg = message.payload['message']

        # Ignore anything that isn't a command to us.
        if not msg.startswith(self.command_prefix):
            return

        command, params = safe_split(msg[len(self.command_prefix):], None, 1)
        cmd = getattr(self, 'cmd_'+command.lower(), None)
        if cmd:
            resp = cmd(params)
        else:
            resp = "No such command: %s" % (command,)

        if resp:
            self.publisher.publish_message(Message(recipient=self.name, message=resp))

    def cmd_load(self, params):
        params = params.split()

        if not params:
            return "Please provide a worker class to load."
        worker_class = params.pop(0)

        config = {}
        try:
            for param in params:
                k, v = param.split('=', 1)
                config[k] = v
        except:
            return "Invalid parameters."

        try:
            self.load_worker(worker_class, config)
            return "Loaded worker class %s with config %r" % (worker_class, config)
        except Exception, e:
            return "Error loading worker class: %r" % (e,)

    def stopWorker(self):
        log.msg("Stopping console worker.")
