# -*- test-case-name: vumi.demos.tests.test_ircbot -*-

"""Demo workers for constructing a simple IRC bot."""

import re

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.application import ApplicationWorker


class MemoWorker(ApplicationWorker):
    """Watches for memos to users and notifies users of memos when users
    appear.

    Configuration
    -------------
    transport_name : str
        Name of the transport.
    """

    MEMO_RE = re.compile(r'^\S+ tell (\S+) (.*)$')

    # TODO: store memos in redis

    @inlineCallbacks
    def startWorker(self):
        self.memos = {}
        yield super(MemoWorker, self).startWorker()

    def consume_user_message(self, msg):
        """Log message from a user."""
        nickname = msg.user()
        irc_metadata = msg['helper_metadata'].get('irc', {})
        channel = irc_metadata.get('irc_channel', 'unknown')
        addressed_to = irc_metadata.get('addressed_to_transport', True)

        if addressed_to:
            self.process_potential_memo(channel, nickname, msg)

        memos = self.memos.pop((channel, nickname), [])
        if memos:
            log.msg("Time to deliver some memos:", memos)
        for memo_sender, memo_text in memos:
            self.reply_to(msg, "message from %s: %s" % (memo_sender,
                                                        memo_text))

    def process_potential_memo(self, channel, nickname, msg):
        match = self.MEMO_RE.match(msg['content'])
        if match:
            recipient = match.group(1).lower()
            memo_text = match.group(2)
            self.memos.setdefault((channel, recipient),
                                  []).append((nickname, memo_text))
            self.reply_to(msg, "Sure thing, boss.")
