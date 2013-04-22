from twisted.internet.defer import inlineCallbacks

from vumi.application import ApplicationWorker


class SmitterApplication(ApplicationWorker):

    def setup_application(self):
        self.members = set()

    @inlineCallbacks
    def consume_user_message(self, message):
        if message['content'] == '+':
            self.members.add(message.user())
            yield self.reply_to(
                message, 'You have joined Smitter! SMS "-" to leave.')
        elif message['content'] == '-':
            self.members.remove(message.user())
            yield self.reply_to(
                message, 'You have left Smitter. SMS "+" to join.')
        else:
            yield self.reply_to(
                message, 'Broadcast to %s members' % (len(self.members) - 1,))
            for member in self.members:
                if member != message.user():
                    yield self.send_to(member, message['content'])
