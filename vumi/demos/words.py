"""Demo ApplicationWorkers that perform simple text manipulations."""

from twisted.python import log

from vumi.message import Message
from vumi.application import ApplicationWorker


class SimpleAppWorker(ApplicationWorker):
    """Base class for very simple application workers.

       Configuration
       -------------
       transport_name : str
           Name of the transport.
       """

    def consume_user_message(self, msg):
        """Find or create a hangman game for this player.

        Then process the user's message.
        """
        log.msg("User message: %s" % msg['content'])
        text = msg['content']
        if text is None:
            return
        reply = self.process_message(text)
        self.reply_to(msg, reply)

    def consume_message(self, message):
        log.msg("Consumed message %s" % message)
        data = self.process_message(message.payload['message'])
        if data:
            self.publisher.publish_message(Message(recipient=self.name,
                                                   message=data))

    def process_message(self, text):
        raise NotImplementedError("Sub-classes should implement"
                                  " process_message.")


class EchoWorker(SimpleAppWorker):
    """Echos text back to the sender."""

    def process_message(self, data):
        return data


class ReverseWorker(SimpleAppWorker):
    """Replies with reversed text."""

    def process_message(self, data):
        return data[::-1]


class WordCountWorker(SimpleAppWorker):
    """Returns word and letter counts to the sender."""

    def process_message(self, data):
        response = []
        words = len(data.split())
        response.append("%s word%s" % (words, "s" * (words != 1)))
        chars = len(data)
        response.append("%s char%s" % (chars, "s" * (chars != 1)))
        return ', '.join(response)
