# -*- test-case-name: vumi.demos.tests.test_logger -*-

"""Demo ApplicationWorker that logs messages received."""

import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.application import ApplicationWorker
from vumi.utils import http_request


class LoggerWorker(ApplicationWorker):
    """Logs text to a URL.

    Configuration
    -------------
    transport_name : str
        Name of the transport.
    log_server : str
        URL to post log messages too.
        E.g.
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
