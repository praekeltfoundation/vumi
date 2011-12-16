# -*- test-case-name: vumi.demos.tests.test_memo -*-

"""Demo ApplicationWorkers that remembers messages for a user."""

import re

from twisted.python import log

from vumi.application import ApplicationWorker


class LoggerWorker(ApplicationWorker):
    """Watches for memos to users and notifies users of memos when users
    appear.

    Configuration
    -------------
    transport_name : str
        Name of the transport.
    """

    MEMO_RE = re.compile(r'^\S+ tell (\S+) (.*)$')

    # TODO: store memos in redis

    def startWorker(self):
        self.memos = {}

    def consume_user_message(self, msg):
        """Log message from a user."""
        log.msg("User message: %s" % msg['content'])
        text = msg['content']

        msg_type = payload['message_type']
        msg = payload['message_content']
        nickname = payload['nickname']
        channel = payload.get('channel', 'unknown')

        log.msg("Got message:", payload)

        if msg_type == 'message' and payload["addressed"]:
            log.msg("Looks like something I should process.")
            yield self.process_potential_memo(channel, nickname, msg, payload)

        memos = self.memos.pop((channel, nickname), [])
        if memos:
            log.msg("Time to deliver some memos:", memos)
        for memo in memos:
            msg = "%s: message from %s: %s" % (nickname, memo[0], memo[1])
            yield self._publish_message(message_type='message',
                                        channel=channel, msg=msg,
                                        server=payload['server'])

    @inlineCallbacks
    def process_potential_memo(self, channel, nickname, message, payload):
        match = self.MEMO_RE.match(message)
        if match:
            self.memos.setdefault((channel, match.group(1)), []).append(
                (nickname, match.group(2)))
            msg = "Sure thing, boss."
            yield self._publish_message(message_type='message',
                                        channel=channel, msg=msg,
                                        server=payload['server'])
