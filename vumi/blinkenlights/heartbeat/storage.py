# -*- test-case-name: vumi.blinkenlights.heartbeat.tests.test_monitor -*-

"""
Storage Schema:

 Systems (set):            key = systems
 Workers in system (set):  key = system:$SYSTEM_ID:workers
 Worker attributes (hash): key = worker:$WORKER_ID:attrs
 Worker hostinfo (hash):   key = worker:$WORKER_ID:hosts
 Worker Issue (hash):      key = worker:$WORKER_ID:issue
"""


# some redis key-making functions.
#
# These functions are reused by the monitoring dash,
# which uses another redis client interface
# Its a lot simpler for now to make these toplevel functions.

def attr_key(worker_id):
    return "worker:%s:attrs" % worker_id


def hostinfo_key(worker_id):
    return "worker:%s:hosts" % worker_id


def issue_key(worker_id):
    return "worker:%s:issue" % worker_id


class Storage(object):
    """
    TxRedis interface for the heartbeat monitor. Basically only
    supports mutating operations since the monitor does not do
    any reads
    """

    def __init__(self, redis):
        self._redis = redis
        self.manager = redis

    def set_systems(self, system_ids):
        """ delete existing system ids and replace with new ones """
        key = "systems"
        self._redis.delete(key)
        for sys_id in system_ids:
            self._redis.sadd(key, sys_id)

    def set_system_workers(self, system_id, worker_ids):
        """ delete existing worker ids and replace with new ones """
        key = "system:%s:workers" % system_id
        self._redis.delete(key)
        # not sure how to make sadd() accept varags :-(
        for wkr_id in worker_ids:
            self._redis.sadd(key, wkr_id)

    def set_worker_attrs(self, worker_id, attrs):
        key = attr_key(worker_id)
        self._redis.hmset(key, attrs)

    def set_worker_hostinfo(self, worker_id, hostinfo):
        key = hostinfo_key(worker_id)
        self._redis.hmset(key, hostinfo)

    def delete_worker_hostinfo(self, worker_id):
        key = hostinfo_key(worker_id)
        self._redis.delete(key)

    def set_worker_issue(self, worker_id, issue):
        key = issue_key(worker_id)
        self._redis.hmset(key, issue)

    def delete_worker_issue(self, worker_id):
        key = issue_key(worker_id)
        self._redis.delete(key)

    def open_or_update_issue(self, worker_id, issue):
        key = issue_key(worker_id)
        # add these fields if they do not already exist
        self._redis.hsetnx(key, 'issue_type', issue['issue_type'])
        self._redis.hsetnx(key, 'start_time', issue['start_time'])
        # update current proc count
        self._redis.hset(key, 'procs_count', issue['procs_count'])
