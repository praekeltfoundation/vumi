from vumi.application import ApplicationWorker


class SmitterApplication(ApplicationWorker):

    def setup_application(self):
        return self.send_to(<your phone number here>, 'hi there!')

    def consume_user_message(self, message):
        return self.reply_to(message, 'thanks!')
