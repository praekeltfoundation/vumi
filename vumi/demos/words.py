# -*- test-case-name: vumi.demos.tests.test_words -*-

"""Demo ApplicationWorkers that perform simple text manipulations."""

from twisted.python import log

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
        content = msg['content'].encode('utf-8') if msg['content'] else None
        log.msg("User message: %s" % content)
        text = msg['content']
        if text is None:
            reply = self.get_help()
        else:
            reply = self.process_message(text)
        return self.reply_to(msg, reply)

    def process_message(self, text):
        raise NotImplementedError("Sub-classes should implement"
                                  " process_message.")

    def get_help(self):
        return "Enter text:"


class EchoWorker(SimpleAppWorker):
    """Echos text back to the sender."""

    def process_message(self, data):
        return data

    def get_help(self):
        return "Enter text to echo:"


class ReverseWorker(SimpleAppWorker):
    """Replies with reversed text."""

    def process_message(self, data):
        return data[::-1]

    def get_help(self):
        return "Enter text to reverse:"


class WordCountWorker(SimpleAppWorker):
    """Returns word and letter counts to the sender."""

    def process_message(self, data):
        response = []
        words = len(data.split())
        response.append("%s word%s" % (words, "s" * (words != 1)))
        chars = len(data)
        response.append("%s char%s" % (chars, "s" * (chars != 1)))
        return ', '.join(response)

    def get_help(self):
        return "Enter text to return word and character counts for:"
