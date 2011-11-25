# -*- test-case-name: vumi.workers.ttc.tests.test_ttc -*-

from twisted.python import log

from vumi.application import ApplicationWorker

class TtcGenericWorker(ApplicationWorker):
    
    def consume_user_message(self, message):
        log.msg("User message: %s" % msg['content'])