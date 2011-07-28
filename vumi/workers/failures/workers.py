# -*- test-case-name: vumi.workers.failures.tests.test_workers -*-

import redis
from twisted.python import log

from vumi.service import Worker
from vumi.utils import get_deploy_int


class Vas2NetsFailureWorker(Worker):
    """
    Handler for transport failures in the Vas2Nets transport.
    """

    def startWorker(self):
        # Connect to Redis
        self.r_server = redis.Redis("localhost",
                                    db=get_deploy_int(self._amqp_client.vhost))
        log.msg("Connected to Redis")
        self.r_prefix = ":".join(["failures", self.config['transport_name']])
        log.msg("r_prefix = %s" % self.r_prefix)

    def r_set(self, key, value):
        self.r_server.set("#".join((self.r_prefix, key)), value)

    def r_get(self, key):
        return self.r_server.get("#".join((self.r_prefix, key)))

    def log_failure(self, failure):
        self.r_set("failure", failure)

