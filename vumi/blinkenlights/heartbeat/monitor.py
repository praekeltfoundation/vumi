# -*- test-case-name: vumi.blinkenlights.tests.test_heartbeat -*-

import time
import collections

from twisted.internet.defer import inlineCallbacks
from twisted.web import server
from twisted.application import service, internet
from twisted.internet.task import LoopingCall
from txjsonrpc.web import jsonrpc

from vumi.service import Worker
from vumi.log import log
from vumi.blinkenlights.heartbeat.publisher import HeartBeatMessage


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
        data = {}
        for system_id, workers in self._state.iteritems():
            data[system_id] = {}
            for worker_id, wkr in workers.iteritems():
                wkr_js = data[system_id][worker_id] = {}
                wkr_js['worker_id'] = wkr['worker_id']
                wkr_js['hostname'] = wkr['hostname']
                wkr_js['pid'] = wkr['pid']
                wkr_js['events'] = []
                for ev in wkr['events']:
                    wkr_js['events'].append({
                        'state': ev.state,
                        'timestamp': ev.timestamp,
                    })
        return data


class Event(object):
    """
    Represents a change in the recorded state of a worker
    """

    ALIVE = "ALIVE"
    MISSING = "MISSING"

    def __init__(self, timestamp, state):
        self.timestamp = timestamp
        self.state = state


class HeartBeatMonitor(Worker):

    deadline = 30
    data_port = 7080

    @inlineCallbacks
    def startWorker(self):
        log.msg("Heartbeat monitor initializing")

        self.data_port = self.config.get("data_server_port",
                                    HeartBeatMonitor.data_port)
        self.deadline = self.config.get("deadline",
                                        HeartBeatMonitor.deadline)

        log.msg("heartbeat-data-server running on port %s" % self.data_port)

        self._state = collections.defaultdict(dict)

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
        log.msg("HeartBeat: stopping")
        if self._task:
            self._task.stop()
            self._task = None

    def _ensure(self, system_id, worker_id):
        """
        Make sure there is an entry for worker data in the in-memory db.
        Returns a reference to the worker node.
        """
        if worker_id not in self._state[system_id]:
            wkr = {
                    'events': [],
                  }
            self._state[system_id][worker_id] = wkr
        return self._state[system_id][worker_id]

    def update(self, msg):
        """
        Process a heartbeat message
        """
        system_id = msg.get('system_id', None)
        worker_id = msg.get('worker_id', None)

        # discard if msg format isn't valid
        if not system_id or not worker_id:
            return

        # ensure there is an entry for the worker in the db
        wkr = self._ensure(system_id, worker_id)

        # update the worker's known state
        wkr['system_id'] = msg['system_id']
        wkr['worker_id'] = msg['worker_id']
        wkr['hostname'] = msg['hostname']
        wkr['pid'] = msg['pid']
        wkr['timestamp'] = msg['timestamp']

        # Add an event if necessary
        if (not wkr['events']) or wkr['events'][-1].state == Event.MISSING:
            ev = Event(msg['timestamp'], Event.ALIVE)
            wkr['events'].append(ev)

    def _find_missing_workers(self, deadline):
        lst = []
        for system_id, workers in self._state.iteritems():
            for wkr in [wkr for wkr in workers.values()
                        if wkr['timestamp'] < deadline]:
                if wkr['events'][-1].state == Event.ALIVE:
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
