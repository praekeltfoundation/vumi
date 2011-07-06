from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from vumi.service import Worker, WorkerCreator
from vumi.message import Message
from vumi.utils import load_class_by_string
import vumi.options


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
        words = len(data.split())
        chars = len(data)
        return "%s word%s, %s char%s" % (words, "s" * (words != 1), len(data), "s" * (chars != 1))


class TelnetConsoleWorkerCreator(TelnetConsoleWorker):
    name = 'workercreator'

    def process_message(self, data):
        if not data.startswith("!load "):
            return None
        data = data.split()[1]
        try:
            worker_class = load_class_by_string(data.strip())
            creator = WorkerCreator(worker_class, **vumi.options.get_all())
            # after that you connect it to the AMQP server
            creator.connectTCP("127.0.0.1", 5672)
            return "Created new worker."
        except Exception, e:
            return "Error loading new worker: %r" % (e,)

