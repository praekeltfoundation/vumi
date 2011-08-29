# -*- test-case-name: vumi.workers.vas2nets.tests.test_failures -*-

from vumi.workers.failures.workers import FailureWorker


class Vas2NetsFailureWorker(FailureWorker):

    def do_retry(self, message, reason):
        message = self.update_retry_metadata(message)
        self.store_failure(message, reason, message['retry_metadata']['delay'])

    def handle_failure(self, message, reason):
        if reason == "connection refused":
            self.do_retry(message, reason)
        else:
            self.store_failure(message, reason)
