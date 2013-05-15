# -*- test-case-name: vumi.blinkenlights.tests.test_heartbeat -*-

"""
Storage Schema:

 List of systems (JSON list): key = systems
 System state (JSON dict):    key = $SYSTEM_ID
 Worker issue (JSON dict):    key = worker:$WORKER_ID:issue
"""

import json

from vumi.persist.redis_base import Manager

SYSTEMS_KEY = "systems"


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
        yield self._redis.set(SYSTEMS_KEY, json.dumps(system_ids))

    @Manager.calls_manager
    def write_system(self, sys):
        yield self._redis.set(sys.system_id, sys.dumps())

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
