import json
from datetime import datetime
from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from vumi.service import Worker
from vumi.message import Message


class IRCWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting IRC worker.")
        inbound = self.config.get('inbound', 'irc.inbound')
        outbound = self.config.get('outbound', 'irc.outbound')
        self.publisher = yield self.publish_to(outbound)
        self.consume(inbound, self.consume_message, '%s.%s' % (inbound, self.name))
        self.worker_setup()
        log.msg("Started service")

    def _publish_message(self, **kwargs):
        timestamp = datetime.utcnow()

        payload = {
            "nickname": kwargs.get('nickname', 'system'),
            "server": kwargs.get('server', 'unknown'),
            "channel": kwargs.get('channel', 'unknown'),
            "message_type": kwargs.get('message_type', 'message'),
            "message_content": kwargs.get('msg', ''),
            "timestamp": timestamp.isoformat()
        }

        self.publisher.publish_message(Message(
                recipient=self.name, **payload))

    def consume_message(self, message):
        log.msg("Consumed message %s" % message)
        data = self.process_message(message.payload)
        if data:
            self.publisher.publish_message(Message(recipient=self.name, message=data))

    def process_message(self, data):
        return None

    def stopWorker(self):
        log.msg("Stopping IRC worker.")


class MessageLogger(IRCWorker):
    name = 'message_logger'

    def worker_setup(self):
        self.log_server = self.config.get('log_server', 'http://example.com/')

    def log(self, **kwargs):
        """Write a message to the file."""
        # utils.post_data_to_url(self.log_server, json.dumps(payload), 'application/json')
        log.msg("payload: %r" % (kwargs,))

    def process_message(self, payload):
        msg_type = payload['message_type']
        msg = payload['message_content']
        nickname = payload['nickname']
        channel = payload.get('channel', 'unknown')

        # Check to see if they're sending me a private message
        if not any(channel.startswith(p) for p in ('#', '&', '$')):
            msg = "It isn't nice to whisper!  Play nice with the group."
            self._publish_message(message_type='message', channel=nickname,
                                  msg=msg, server=payload['server'])
            return
        if msg_type in ('message', 'system'):
            self.log(nickname=nickname, channel=channel, msg=msg)
        elif msg_type == 'action':
            self.log(message_type=msg_type, channel=channel, msg="* %s %s" % (nickname, msg))
        elif msg_type == 'nick_change':
            self.log(message_type='system', msg="%s is now known as %s" % (nickname, msg))
