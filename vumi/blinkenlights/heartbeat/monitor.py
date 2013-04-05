# -*- test-case-name: vumi.blinkenlights.tests.test_heartbeat -*-

import time

from twisted.internet.defer import inlineCallbacks
from twisted.web import server
from twisted.application import service, internet
from twisted.internet.task import LoopingCall
from txjsonrpc.web import jsonrpc

from vumi.service import Worker
from vumi.log import log
from vumi.blinkenlights.heartbeat.publisher import HeartBeatMessage
from vumi.errors import ConfigError


class RPCServer(jsonrpc.JSONRPC):
    """
    Very simple JSON data provider the Web Dash.

    Exports a single 'get_state()' method which provides the state tree in
    serialized form.

    This will be replaced by a proper datastore, probably redis, in the next
    iteration.
    """

    def __init__(self, state):
        jsonrpc.JSONRPC.__init__(self)
        self._state = state

    def jsonrpc_get_state(self, ver):
        return self.serialize_state()

    def serialize_state(self):
        """
        Walk the tree and transform objects into JSON hashes
        """
        jsn = {}
        for system_id, system_data in self._state.iteritems():
            jsn[system_id] = {}
            for worker_id, w in system_data['workers'].iteritems():
                wkr = jsn[system_id][worker_id] = {}
                wkr['system_id'] = w['system_id']
                wkr['worker_id'] = w['worker_id']
                wkr['worker_name'] = w['worker_name']
                wkr['hostname'] = w['hostname']
                wkr['pid'] = w['pid']
                wkr['timestamp'] = w['timestamp']
                wkr['events'] = []
                for ev in w['events']:
                    wkr['events'].append(
                        {
                            'state': ev.state,
                            'timestamp': ev.timestamp,
                        })
        return jsn


class Event(object):
    """
    Represents a change in the recorded state of a worker
    """

    ALIVE = "ALIVE"
    MISSING = "MISSING"
    START = "START"

    def __init__(self, timestamp, state):
        self.timestamp = timestamp
        self.state = state


class HeartBeatMonitor(Worker):

    deadline = 30
    data_port = 7080

    # Instance vars:
    #
    # _state: Nested dict storing information about monitored objects
    #

    @inlineCallbacks
    def startWorker(self):
        log.msg("Heartbeat monitor initializing")

        self.data_port = self.config.get("data_server_port",
                                    HeartBeatMonitor.data_port)
        self.deadline = self.config.get("deadline",
                                        HeartBeatMonitor.deadline)

        self._build_worker_tree(self.config)
        log.msg("heartbeat-data-server running on port %s" % self.data_port)

        # Start consuming heartbeats
        yield self.consume("heartbeat.inbound", self.consume_message,
                           exchange_name='vumi.health',
                           message_class=HeartBeatMessage)

        # Start the JSON-RPC server
        root = RPCServer(self._state)
        site = server.Site(root)
        rpc_service = internet.TCPServer(self.data_port, site)
        rpc_service.setServiceParent(
            service.Application("heartbeat-data-server")
        )
        self.addService(rpc_service)

        self._start_looping_task()

    def stopWorker(self):
        log.msg("HeartBeat: stopping worker")
        if self._task:
            self._task.stop()
            self._task = None

    def _assert_field(self, cfg, key):
        if key not in cfg:
            raise ConfigError("Expected '%s' field in config" % key)

    def _build_worker_tree(self, config):
        """
        Build a tree of worker objects based on the config
        """
        self._assert_field(config, 'monitored_systems')
        systems = config.get("monitored_systems", None)
        self._state = state = {}
        # loop over each defined system
        for sys in systems.values():
            self._assert_field(sys, 'workers')
            self._assert_field(sys, 'system_id')
            system_id = sys['system_id']
            state[system_id] = {}
            state[system_id]['workers'] = wkrs = {}
            # loop over each defined worker in the system
            for wkr in sys['workers'].values():
                self._assert_field(wkr, 'worker_name')
                self._assert_field(wkr, 'max_procs')
                name = wkr['worker_name']
                max_procs = wkr['max_procs']
                # create an entry for each worker from 0 to max_procs
                for i in range(0, max_procs):
                    worker_id = "%s.%s" % (name, i)
                    wkrs[worker_id] = self._init_worker(system_id,
                                                        worker_id,
                                                        name,
                                                        i)

    def _init_worker(self, system_id, worker_id, name, number):
        wkr = {}
        tm = time.time()
        wkr['system_id'] = system_id
        wkr['worker_id'] = worker_id
        wkr['hostname'] = None
        wkr['pid'] = None
        wkr['worker_name'] = name
        wkr['timestamp'] = tm
        wkr['events'] = [Event(tm, Event.START)]
        return wkr

    def _lookup_worker(self, system_id, worker_id):
        """
        Returns a reference to the worker node or None.
        """
        if system_id not in self._state:
            return None
        if worker_id not in self._state[system_id]['workers']:
            return None
        return self._state[system_id]['workers'][worker_id]

    def update(self, msg):
        """
        Process a heartbeat message
        """

        # extract mandatory fields
        version = msg.get('version', None)
        system_id = msg.get('system_id', None)
        worker_id = msg.get('worker_id', None)
        timestamp = msg.get('timestamp', None)

        # invalid/obsolete msg, so discard
        if (not system_id) or (not worker_id):
            return

        # Discard msg if wkr is unknown
        wkr = self._lookup_worker(system_id, worker_id)
        if wkr is None:
            return

        # update the worker's known state
        wkr['system_id'] = msg['system_id']
        wkr['worker_id'] = msg['worker_id']
        wkr['worker_name'] = msg.get('worker_name', None)
        wkr['worker_number'] = msg['worker_number']
        wkr['hostname'] = msg['hostname']
        wkr['pid'] = msg['pid']
        wkr['timestamp'] = msg['timestamp']

        # Add an event if necessary
        state = wkr['events'][-1].state
        if state == Event.START or state == Event.MISSING:
            ev = Event(msg['timestamp'], Event.ALIVE)
            wkr['events'].append(ev)

    def _find_missing_workers(self, deadline):
        lst = []
        systems = self._state.values()
        for sys in systems:
            for wkr in sys['workers'].values():
                if wkr['timestamp'] < deadline:
                    state = wkr['events'][-1].state
                    if state == Event.START or state == Event.ALIVE:
                        lst.append(wkr)
        return lst

    def _process_missing(self, workers):
        for wkr in workers:
            log.msg("worker is now missing: %s" % wkr['worker_id'])
            ev = Event(wkr['timestamp'], Event.MISSING)
            wkr['events'].append(ev)

    def _check_missing(self):
        """
        Iterate over worker records and check to see whether any have not
        checked-in on time.
        """
        deadline = time.time() - self.deadline
        self._process_missing(self._find_missing_workers(deadline))

    def _start_looping_task(self):
        """ Create a timer task to check for missing worker heartbeats """
        self._task = LoopingCall(self._check_missing)
        done = self._task.start(self.deadline, now=False)
        done.addErrback(lambda failure: log.err(failure, "timer task died"))

    def consume_message(self, msg):
        log.msg("Received message: %s" % msg)
        self.update(msg.payload)
