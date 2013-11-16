# -*- test-case-name: vumi.demos.tests.test_static_reply -*-
from vumi.application import ApplicationWorker
from vumi.config import ConfigText


class StaticReplyConfig(ApplicationWorker.CONFIG_CLASS):
    reply_text = ConfigText(
        "Reply text to send in response to inbound messages.", static=True)


class StaticReplyApplication(ApplicationWorker):
    """
    Application that replies to incoming messages with a configured response.
    """
    CONFIG_CLASS = StaticReplyConfig

    def consume_user_message(self, message):
        config = self.get_static_config()
        if config.reply_text:
            return self.reply_to(
                message, config.reply_text, continue_session=False)
