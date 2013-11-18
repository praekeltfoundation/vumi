# -*- test-case-name: vumi.demos.tests.test_static_reply -*-
from twisted.internet.defer import succeed, inlineCallbacks, returnValue

from vumi.application import ApplicationWorker
from vumi.config import ConfigText


class StaticReplyConfig(ApplicationWorker.CONFIG_CLASS):
    reply_text = ConfigText(
        "Reply text to send in response to inbound messages.", static=False)


class StaticReplyApplication(ApplicationWorker):
    """
    Application that replies to incoming messages with a configured response.
    """
    CONFIG_CLASS = StaticReplyConfig

    def get_config(self, message, ctxt=None):
        config = self.config.copy()
        if 'reply_text' in config:
            config['reply_text'] = '%s %s' % (
                config['reply_text'], message.user())
        return succeed(self.CONFIG_CLASS(config))

    @inlineCallbacks
    def consume_user_message(self, message):
        config = yield self.get_config(message)
        if config.reply_text:
            yield self.reply_to(
                message, config.reply_text, continue_session=False)
