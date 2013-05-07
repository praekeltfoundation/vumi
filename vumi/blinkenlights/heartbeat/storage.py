# -*- test-case-name: vumi.blinkenlights.tests.test_heartbeat -*-

"""
Storage Schema:

 Systems (set):            key = systems
 Workers in system (set):  key = system:$SYSTEM_ID:workers
 Worker attributes (hash): key = worker:$WORKER_ID:attrs
 Worker hostinfo (hash):   key = worker:$WORKER_ID:hosts
 Worker Issue (hash):      key = worker:$WORKER_ID:issue
"""

from vumi.persist.redis_base import Manager


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

    @Manager.calls_manager
    def set_systems(self, system_ids):
        """ delete existing system ids and replace with new ones """
        key = "systems"
        yield self._redis.delete(key)
        for sys_id in system_ids:
            yield self._redis.sadd(key, sys_id)

    @Manager.calls_manager
    def set_system_workers(self, system_id, worker_ids):
        """ delete existing worker ids and replace with new ones """
        key = "system:%s:workers" % system_id
        yield self._redis.delete(key)
        yield self._redis.sadd(key, *worker_ids)

    @Manager.calls_manager
    def set_worker_attrs(self, worker_id, wkr):
        key = attr_key(worker_id)
        attrs = {
            'system_id': wkr.system_id,
            'worker_id': wkr.worker_id,
            'worker_name': wkr.worker_name,
            'min_procs': wkr.min_procs,
        }
        yield self._redis.hmset(key, attrs)

    @Manager.calls_manager
    def set_worker_hostinfo(self, worker_id, hostinfo):
        key = hostinfo_key(worker_id)
        # delete existing hosts, otherwise stale host data may remain
        yield self._redis.delete(key)
        yield self._redis.hmset(key, hostinfo)

    @Manager.calls_manager
    def delete_worker_hostinfo(self, worker_id):
        key = hostinfo_key(worker_id)
        yield self._redis.delete(key)

    @Manager.calls_manager
    def set_worker_issue(self, worker_id, issue):
        key = issue_key(worker_id)
        yield self._redis.hmset(key, issue)

    @Manager.calls_manager
    def delete_worker_issue(self, worker_id):
        key = issue_key(worker_id)
        yield self._redis.delete(key)

    @Manager.calls_manager
    def open_or_update_issue(self, worker_id, issue):
        key = issue_key(worker_id)
        # add these fields if they do not already exist
        yield self._redis.hsetnx(key, 'issue_type', issue.issue_type)
        yield self._redis.hsetnx(key, 'start_time', issue.start_time)
        # update current proc count
        yield self._redis.hset(key, 'procs_count', issue.procs_count)
