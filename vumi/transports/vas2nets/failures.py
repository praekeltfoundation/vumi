# -*- test-case-name: vumi.transports.vas2nets.tests.test_failures -*-

from vumi.transports.failures import FailureWorker, FailureMessage


class Vas2NetsFailureWorker(FailureWorker):

    def do_retry(self, message, reason):
        message = self.update_retry_metadata(message)
        self.store_failure(message, reason, message['retry_metadata']['delay'])

    def handle_failure(self, message, failure_code, reason):
        if failure_code == FailureMessage.FC_TEMPORARY:
            self.do_retry(message, reason)
        else:
            self.store_failure(message, reason)
