# -*- test-case-name: vumi.demos.tests.test_ircbot -*-

"""Demo workers for constructing a simple IRC bot."""

import re
import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.application import ApplicationWorker
from vumi.utils import http_request


class LoggerWorker(ApplicationWorker):
    """Logs messages to a URL.

    Configuration
    -------------
    transport_name : str
        Name of the transport.
    log_server : str
        URL to post log messages too.
        E.g. http://ircarchive.appspot.com/bot/
    """

    def startWorker(self):
        """Start the worker"""
        self.log_server_url = self.config['log_server']
        super(LoggerWorker, self).startWorker()

    @inlineCallbacks
    def log(self, **kwargs):
        """Write a message to web logging API."""
        log.msg("Logging payload: %r" % (kwargs,))
        headers = {
            'Content-Type': ['application/json'],
            }
        response = yield http_request(self.log_server_url,
                                      json.dumps(kwargs),
                                      headers=headers)
        log.msg("Response: %r" % (response,))

    @inlineCallbacks
    def consume_user_message(self, msg):
        """Log message from a user."""
        user = msg.user()
        transport_metadata = msg['transport_metadata']
        irc_channel = transport_metadata.get('irc_channel')
        irc_command = transport_metadata.get('irc_command', 'PRIVMSG')
        text = msg['content']

        # only log messages send to a channel
        if irc_channel is None:
            return

        nickname = user.partition('!')[0]

        if irc_command == 'PRIVMSG':
            yield self.log(message_type='message', nickname=nickname,
                           channel=irc_channel, msg=text)
        elif irc_command == 'ACTION':
            yield self.log(message_type='action', channel=irc_channel,
                           msg="* %s %s" % (nickname, text))


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
        user = msg.user()
        transport_metadata = msg['transport_metadata']
        channel = transport_metadata.get('irc_channel', 'unknown')
        addressed_to = transport_metadata.get('irc_addressed_to_transport',
                                              True)

        if addressed_to:
            self.process_potential_memo(channel, user, msg)

        memos = self.memos.pop((channel, user), [])
        if memos:
            log.msg("Time to deliver some memos:", memos)
        for memo_sender, memo_text in memos:
            self.reply_to(msg, "message from %s: %s" % (memo_sender,
                                                        memo_text))

    def process_potential_memo(self, channel, nickname, msg):
        match = self.MEMO_RE.match(msg['content'])
        if match:
            self.memos.setdefault((channel, match.group(1)), []).append(
                (nickname, match.group(2)))
            self.reply_to(msg, "Sure thing, boss.")
