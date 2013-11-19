# -*- test-case-name: vumi.demos.tests.test_static_reply -*-
from datetime import date

from twisted.internet.defer import succeed, inlineCallbacks

from vumi.application import ApplicationWorker
from vumi.config import ConfigText


class StaticReplyConfig(ApplicationWorker.CONFIG_CLASS):
    reply_text = ConfigText(
        "Reply text to send in response to inbound messages.", static=False,
        default="Hello {user} at {now}.")


class StaticReplyApplication(ApplicationWorker):
    """
    Application that replies to incoming messages with a configured response.
    """
    CONFIG_CLASS = StaticReplyConfig

    @inlineCallbacks
    def consume_user_message(self, message):
        config = yield self.get_config(message)
        yield self.reply_to(
            message, config.reply_text.format(
                user=message.user(), now=date.today()),
            continue_session=False)
