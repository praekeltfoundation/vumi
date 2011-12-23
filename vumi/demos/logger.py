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
        response = yield http_request(self.log_server_url,
                                      json.dumps(kwargs),
                                      headers={
                                          'Content-Type': 'application/json'
                                          })
        log.msg("Response: %r" % (response,))

    @inlineCallbacks
    def consume_user_message(self, msg):
        """Log message from a user."""
        user = msg.user()
        transport_metadata = msg['transport_metadata']
        channel = transport_metadata.get('irc_channel', 'unknown')

        text = msg['content']


        # Check to see if they're sending me a private message
        if not any(channel.startswith(p) for p in ('#', '&', '$')):
            return
        if msg_type in ('message', 'system'):
            self.log(nickname=nickname, channel=channel, msg=msg)
        elif msg_type == 'action':
            self.log(message_type=msg_type, channel=channel,
                     msg="* %s %s" % (nickname, msg))
        elif msg_type == 'nick_change':
            self.log(message_type='system',
                     msg="%s is now known as %s" % (nickname, msg))

