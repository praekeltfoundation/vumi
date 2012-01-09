# -*- test-case-name: vumi.demos.tests.test_ircbot -*-

"""Demo workers for constructing a simple IRC bot."""

import re

from twisted.python import log
import redis
import json

from vumi.application import ApplicationWorker


class MemoWorker(ApplicationWorker):
    """Watches for memos to users and notifies users of memos when users
    appear.

    Configuration
    -------------
    transport_name : str
        Name of the transport.
    worker_name : str
        Name of this worker. Used as part of the Redis key prefix.
    """

    MEMO_RE = re.compile(r'^\S+ tell (\S+) (.*)$')

    def validate_config(self):
        self.redis_config = self.config.get('redis', {})
        self.r_prefix = "ircbot:memos:%s" % (self.config['worker_name'],)

    def setup_application(self):
        self.r_server = redis.Redis(**self.redis_config)

    def rkey_memo(self, channel, recipient):
        return "%s:%s:%s" % (self.r_prefix, channel, recipient)

    def store_memo(self, channel, recipient, sender, text):
        memo_key = self.rkey_memo(channel, recipient)
        value = json.dumps([sender, text])
        self.r_server.rpush(memo_key, value)

    def retrieve_memos(self, channel, recipient, delete=False):
        memo_key = self.rkey_memo(channel, recipient)
        memos = self.r_server.lrange(memo_key, 0, -1)
        if delete:
            self.r_server.delete(memo_key)
        return [json.loads(value) for value in memos]

    def consume_user_message(self, msg):
        """Log message from a user."""
        nickname = msg.user()
        irc_metadata = msg['helper_metadata'].get('irc', {})
        channel = irc_metadata.get('irc_channel', 'unknown')
        addressed_to = irc_metadata.get('addressed_to_transport', True)

        if addressed_to:
            self.process_potential_memo(channel, nickname, msg)

        memos = self.retrieve_memos(channel, nickname, delete=True)
        if memos:
            log.msg("Time to deliver some memos:", memos)
        for memo_sender, memo_text in memos:
            self.reply_to(msg, "%s, %s asked me tell you: %s"
                          % (nickname, memo_sender, memo_text))

    def process_potential_memo(self, channel, sender, msg):
        match = self.MEMO_RE.match(msg['content'])
        if match:
            recipient = match.group(1).lower()
            memo_text = match.group(2)
            self.store_memo(channel, recipient, sender, memo_text)
            self.reply_to(msg, "%s: Sure thing, boss." % (sender,))
