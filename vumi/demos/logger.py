# -*- test-case-name: vumi.demos.tests.test_logger -*-

"""Demo ApplicationWorker that logs messages received."""

from twisted.python import log

from vumi.application import ApplicationWorker


class LoggerWorker(ApplicationWorker):
    """Logs text to a URL.

       Configuration
       -------------
       transport_name : str
           Name of the transport.
       log_server : str
           URL to post log messages too.
       """

    def startWorker(self):
        """Start the worker"""
        self.log_server_url = self.config['log_server']

    def consume_user_message(self, msg):
        """Log message from a user."""
        log.msg("User message: %s" % msg['content'])
        text = msg['content']

        msg_type = payload['message_type']
        msg = payload['message_content']
        nickname = payload['nickname']
        channel = payload.get('channel', 'unknown')

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

    def log(self, **kwargs):
        """Write a message to the file."""
        post_data_to_url(self.log_server, json.dumps(kwargs),
                         'application/json')
        log.msg("payload: %r" % (kwargs,))
