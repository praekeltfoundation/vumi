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

Worker = collections.namedtuple('Worker',
                                ['system_id',
                                 'worker_id',
                                 'worker_name',
                                 'min_procs'])


def assert_field(cfg, key):
    """
    helper to check whether a config key is defined. Only used for
    verifying dict fields in the new-style configs
    """
    if key not in cfg:
        raise ConfigError("Expected '%s' field in config" % key)


class WorkerInstance(object):
    """
    A hostname, port pair which uniquely identifies a worker instance.
    Made this into an object since we need to be able to perform identity
    operations on the pair. hash(), eq() for example.
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
        self.worker_name = worker_name
        self.min_procs = min_procs
        self.worker_id = generate_worker_id(system_id, worker_name)

        self._instances = None

    def to_dict(self):
        info = self._compute_host_info(self._instances)
        hosts = []
        for host, count in info.iteritems():
            hosts.append({
                    'host': host,
                    'proc_count': count,
            })
        obj = {
            'id': self.id,
            'name': self.name,
            'min_procs': self.min_procs,
            'hosts': hosts,
        }
        return obj

    def _compute_host_info(self, instances):
        """ Compute the number of worker instances running on each host. """
        bins = {}
        # initialize per-host counters
        for ins in instances:
            bins[ins.hostname] = 0
        # update counters for each instance
        for ins in instances:
            bins[ins.hostname] = bins[ins.hostname] + 1
        return bins

    def audit(self):
        # check whether enough workers checked in
        count = len(self._instances)
        if count < self.min_procs:
            issue = WorkerIssue("min-procs-fail", time.time())
            self._storage.open_or_update_issue(self.worker_id, issue)

    def reset(self):
        self._instances = set()

    def record(self, hostname, pid):
        """ Record that process (hostname,pid) checked in """
        self._instances.add(WorkerInstance(hostname, pid))


class System(object):

    def __init__(self, system_name, system_id, workers):
        self.workers = workers

    def to_dict(self):
        obj = {
            'name': self.name,
            'id': self.id,
            'workers': [wkr.to_dict() for wkr in self.workers],
        }
        return obj

    def dumps(self):
        return json.dumps(self.to_dict())

    def get(self, worker_id):
        return self._workers.get(worker_id, None)


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

    # Instance vars:
    #
    # _storage: Thin wrapper around TxRedisManager. Used to persist
    #           worker data to redis
    #
    # _systems: Describes which workers belong to which system
    _task = None

    @inlineCallbacks
    def startWorker(self):
        log.msg("Heartbeat monitor initializing")

        self.deadline = self._static_config.deadline

        redis_config = self._static_config.redis_manager
        self._redis = yield TxRedisManager.from_config(redis_config)
        self._storage = Storage(self._redis)

        systems_config = self._static_config.monitored_systems
        self._systems, self._workers = self.parse_config(systems_config)

        # Start consuming heartbeats
        yield self.consume("heartbeat.inbound", self.consume_message,
                           exchange_name='vumi.health',
                           message_class=HeartBeatMessage)

        self._start_verification_task()

    @inlineCallbacks
    def stopWorker(self):
        log.msg("HeartBeat: stopping worker")
        if self._task:
            self._task.stop()
            self._task = None
            yield self._task_done

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
            systems.append(System(system_workers))
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

    def _sync_to_redis(self):
        """
        Persist the loaded configuration to redis, possibly overwriting
        any existing data.
        """
        # write system ids
        system_ids = [sys.system_id for sys in self._systems]
        self._storage.set_systems(system_ids)
        # dump each system
        for sys in self._systems:
            self._storage.write(sys)

    def _periodic_task(self):
        """
        Iterate over worker instance sets and check to see whether any have not
        checked-in on time.
        """
        # run diagnostic audits on all workers
        for wkr in self._workers():
            wkr.audit()
        # write everything to redis
        self._sync_to_redis()
        # reset interval state
        for wkr in self._workers:
            wkr.reset()

    def _start_task(self):
        """ Create a timer task to check for missing worker """
        self._task = LoopingCall(self._periodic_task)
        self._task_done = self._task.start(self.deadline, now=False)
        errfn = lambda failure: log.err(failure,
                                        "Heartbeat verify: timer task died")
        self._task_done.addErrback(errfn)
        self._setup_instance_sets()

    def consume_message(self, msg):
        log.msg("Received message: %s" % msg)
        self.update(msg.payload)
