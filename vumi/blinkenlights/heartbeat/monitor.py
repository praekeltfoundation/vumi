# -*- test-case-name: vumi.blinkenlights.heartbeat.tests.test_monitor -*-

import time
import collections
import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import LoopingCall

from vumi.worker import BaseWorker
from vumi.config import ConfigDict, ConfigInt
from vumi.blinkenlights.heartbeat.publisher import HeartBeatMessage
from vumi.blinkenlights.heartbeat.storage import Storage
from vumi.persist.txredis_manager import TxRedisManager
from vumi.utils import generate_worker_id
from vumi.errors import ConfigError
from vumi import log


WorkerIssue = collections.namedtuple('WorkerIssue',
                                     ['issue_type',
                                      'start_time',
                                      'procs_count'])


def assert_field(cfg, key):
    """
    helper to check whether a config key is defined. Only used for
    verifying dict fields in the new-style configs
    """
    if key not in cfg:
        raise ConfigError("Expected '%s' field in config" % key)


class WorkerInstance(object):
    """Represents a worker instance.

    A hostname, process id pair uniquely identify a worker instance.
    """

    def __init__(self, hostname, pid):
        self.hostname = hostname
        self.pid = pid

    def __eq__(self, obj):
        if not isinstance(obj, WorkerInstance):
            return NotImplemented
        return (self.hostname == obj.hostname and
                self.pid == obj.pid)

    def __hash__(self):
        return hash((self.hostname, self.pid))


class Worker(object):

    def __init__(self, system_id, worker_name, min_procs):
        self.system_id = system_id
        self.name = worker_name
        self.min_procs = min_procs
        self.worker_id = generate_worker_id(system_id, worker_name)
        self._instances = set()
        self._instances_active = set()
        self.procs_count = 0

    def to_dict(self):
        """Serializes information into basic dicts"""
        counts = self._compute_host_info(self._instances)
        hosts = []
        for host, count in counts.iteritems():
            hosts.append({
                    'host': host,
                    'proc_count': count,
            })
        obj = {
            'id': self.worker_id,
            'name': self.name,
            'system_id': self.system_id,
            'min_procs': self.min_procs,
            'hosts': hosts,
        }
        return obj

    def _compute_host_info(self, instances):
        """Compute the number of worker instances running on each host."""
        counts = {}
        # initialize per-host counters
        for ins in instances:
            counts[ins.hostname] = 0
        # update counters for each instance
        for ins in instances:
            counts[ins.hostname] = counts[ins.hostname] + 1
        return counts

    @inlineCallbacks
    def audit(self, storage):
        """
        Verify whether enough workers checked in.
        Make sure to call snapshot() before running this method
        """
        count = len(self._instances)
        # if there was previously a min-procs-fail, but now enough
        # instances checked in, then clear the worker issue
        if (count >= self.min_procs) and (self.procs_count < self.min_procs):
            yield storage.delete_worker_issue(self.worker_id)
        if count < self.min_procs:
            issue = WorkerIssue("min-procs-fail", time.time(), count)
            yield storage.open_or_update_issue(self.worker_id, issue)
        self.procs_count = count

    def snapshot(self):
        """
        This method must be run before any diagnostic audit and analyses

        What it does is clear the instances_active set in preparation for
        all the instances which will check-in in the next interval.

        All diagnostics are based on the _instances_active set, which
        holds all the instances which checked-in the previous interval.
        """
        self._instances = self._instances_active
        self._instances_active = set()

    def record(self, hostname, pid):
        """Record that process (hostname,pid) checked in."""
        self._instances_active.add(WorkerInstance(hostname, pid))


class System(object):

    def __init__(self, system_name, system_id, workers):
        self.name = system_name
        self.system_id = system_id
        self.workers = workers

    def to_dict(self):
        """Serialize information to basic dicts"""
        obj = {
            'name': self.name,
            'id': self.system_id,
            'timestamp': int(time.time()),
            'workers': [wkr.to_dict() for wkr in self.workers],
        }
        return obj

    def dumps(self):
        """Dump to a JSON string"""
        return json.dumps(self.to_dict())


class HeartBeatMonitor(BaseWorker):

    class CONFIG_CLASS(BaseWorker.CONFIG_CLASS):
        deadline = ConfigInt(
            "Check-in deadline for participating workers",
            required=True, static=True)
        redis_manager = ConfigDict(
            "Redis client configuration.",
            required=True, static=True)
        monitored_systems = ConfigDict(
            "Tree of systems and workers.",
            required=True, static=True)

    _task = None

    @inlineCallbacks
    def startWorker(self):
        log.msg("Heartbeat monitor initializing")
        config = self.get_static_config()

        self.deadline = config.deadline

        redis_config = config.redis_manager
        self._redis = yield TxRedisManager.from_config(redis_config)
        self._storage = Storage(self._redis)

        self._systems, self._workers = self.parse_config(
            config.monitored_systems)

        # Start consuming heartbeats
        yield self.consume("heartbeat.inbound", self._consume_message,
                           exchange_name='vumi.health',
                           message_class=HeartBeatMessage)

        self._start_task()

    @inlineCallbacks
    def stopWorker(self):
        log.msg("HeartBeat: stopping worker")
        if self._task:
            self._task.stop()
            self._task = None
            yield self._task_done
        self._redis.close_manager()

    def parse_config(self, config):
        """
        Parse configuration and populate in-memory state
        """
        systems = []
        workers = {}
        # loop over each defined system
        for sys in config.values():
            assert_field(sys, 'workers')
            assert_field(sys, 'system_id')
            system_id = sys['system_id']
            system_workers = []
            # loop over each defined worker in the system
            for wkr_entry in sys['workers'].values():
                assert_field(wkr_entry, 'name')
                assert_field(wkr_entry, 'min_procs')
                worker_name = wkr_entry['name']
                min_procs = wkr_entry['min_procs']
                wkr = Worker(system_id,
                             worker_name,
                             min_procs)
                workers[wkr.worker_id] = wkr
                system_workers.append(wkr)
            systems.append(System(system_id, system_id, system_workers))
        return systems, workers

    def update(self, msg):
        """
        Process a heartbeat message.
        """
        worker_id = msg['worker_id']
        timestamp = msg['timestamp']
        hostname = msg['hostname']
        pid = msg['pid']

        # A bunch of discard rules:
        # 1. Unknown worker (Monitored workers need to be in the config)
        # 2. Message which are too old.
        wkr = self._workers.get(worker_id, None)
        if wkr is None:
            log.msg("Discarding message. worker '%s' is unknown" % worker_id)
            return
        if timestamp < (time.time() - self.deadline):
            log.msg("Discarding heartbeat from '%s'. Too old" % worker_id)
            return

        wkr.record(hostname, pid)

    @inlineCallbacks
    def _sync_to_storage(self):
        """
        Write systems data to storage
        """
        # write system ids
        system_ids = [sys.system_id for sys in self._systems]
        yield self._storage.add_system_ids(system_ids)
        # dump each system
        for sys in self._systems:
            yield self._storage.write_system(sys)

    @inlineCallbacks
    def _periodic_task(self):
        """
        Iterate over worker instance sets and check to see whether any have not
        checked-in on time.

        We call snapshot() first, since the execution of tasks here is
        interleaved with the processing of worker heartbeat messages.
        """
        # snapshot the the set of checked-in instances
        for wkr in self._workers.values():
            wkr.snapshot()
        # run diagnostic audits on all workers
        for wkr in self._workers.values():
            yield wkr.audit(self._storage)
        # write everything to redis
        yield self._sync_to_storage()

    def _start_task(self):
        """Create a timer task to check for missing worker"""
        self._task = LoopingCall(self._periodic_task)
        self._task_done = self._task.start(self.deadline, now=False)
        errfn = lambda failure: log.err(failure,
                                        "Heartbeat verify: timer task died")
        self._task_done.addErrback(errfn)

    def _consume_message(self, msg):
        log.msg("Received message: %s" % msg)
        self.update(msg.payload)
