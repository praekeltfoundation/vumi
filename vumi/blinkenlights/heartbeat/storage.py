# -*- test-case-name: vumi.blinkenlights.heartbeat.tests.test_monitor -*-

"""
Storage Schema:

 Systems (JSON list):            key = systems
 Workers in system (JSON list):  key = system:$SYSTEM_ID:workers
 Worker attributes (JSON dict):  key = worker:$WORKER_ID:attrs
 Worker hostinfo (JSON dict):    key = worker:$WORKER_ID:hosts
 Worker Issue (JSON dict):       key = worker:$WORKER_ID:issue
"""

import json

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
        yield self._redis.set(key, json.dumps(system_ids))

    @Manager.calls_manager
    def set_system_workers(self, system_id, worker_ids):
        """ delete existing worker ids and replace with new ones """
        key = "system:%s:workers" % system_id
        yield self._redis.set(key, json.dumps(worker_ids))

    @Manager.calls_manager
    def set_worker_attrs(self, worker_id, wkr):
        key = attr_key(worker_id)
        attrs = {
            'system_id': wkr.system_id,
            'worker_id': wkr.worker_id,
            'worker_name': wkr.worker_name,
            'min_procs': wkr.min_procs,
        }
        yield self._redis.set(key, json.dumps(attrs))

    @Manager.calls_manager
    def set_worker_hostinfo(self, worker_id, hostinfo):
        key = hostinfo_key(worker_id)
        yield self._redis.set(key, json.dumps(hostinfo))

    @Manager.calls_manager
    def delete_worker_hostinfo(self, worker_id):
        key = hostinfo_key(worker_id)
        yield self._redis.delete(key)

    @Manager.calls_manager
    def clear_worker_hostinfo(self, worker_id):
        key = hostinfo_key(worker_id)
        yield self._redis.set(key, json.dumps({}))

    def _issue_to_dict(self, issue):
        return {
            'issue_type': issue.issue_type,
            'start_time': issue.start_time,
            'procs_count': issue.procs_count,
        }

    @Manager.calls_manager
    def set_worker_issue(self, worker_id, issue):
        key = issue_key(worker_id)
        yield self._redis.set(key, json.dumps(self._issue_to_dict(issue)))

    @Manager.calls_manager
    def delete_worker_issue(self, worker_id):
        key = issue_key(worker_id)
        yield self._redis.delete(key)

    @Manager.calls_manager
    def open_or_update_issue(self, worker_id, issue):
        key = issue_key(worker_id)
        issue_raw = yield self._redis.get(key)
        if issue_raw is None:
            issue_data = self._issue_to_dict(issue)
        else:
            issue_data = json.loads(issue_raw)
            issue_data['procs_count'] = issue.procs_count
        yield self._redis.set(key, json.dumps(issue_data))
