
from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.web import server
from twisted.application import service, internet

from vumi.service import Worker
from vumi.message import Message

from vumi.blinkenlights.publisher import HeartBeatMessage
from twisted.internet.task import LoopingCall

from txjsonrpc.web import jsonrpc

import json
import time
import os
import socket
from   datetime import datetime

class RPCServer(jsonrpc.JSONRPC):

    """
    Very simple JSON data provider the Web Dash.

    Exports a single 'get_state()' method which provides the state tree in
    serialized form

    The data will be out into a proper datastore at some point and this server will go away
    """


    def __init__(self, state):
        jsonrpc.JSONRPC.__init__(self)
        self._state = state

    def jsonrpc_get_state(self, ver):
        return self.serialize_state()

    #
    # Walk the tree and transform objects into JSON hashes
    def serialize_state(self):
        data = {}
        for system_id, workers in self._state.iteritems():
            data[system_id] = {}
            for worker_id, wkr in workers.iteritems():
                wkr_js = data[system_id][worker_id] = {}
                wkr_js['record'] = { 'worker_id' : wkr['record'].worker_id,
                                     'hostname'  : wkr['record'].hostname,
                                     'pid'       : wkr['record'].pid,
                                   }
                wkr_js['events'] = []
                for ev in wkr['events']:
                    wkr_js['events'].append({ 'state'     : ev.state,
                                              'timestamp' : ev.timestamp,
                                            })
        return data

class Record(object):

    """
    A node that stores the most recent information about a worker, including
    the latest heartbeat timestamp
    """

    def __init__(self, system_id, worker_id, hostname, pid, timestamp):
        self.system_id  = system_id
        self.worker_id  = worker_id
        self.hostname   = hostname
        self.pid        = pid
        self.timestamp  = timestamp

class Event(object):

    ALIVE   = "ALIVE"
    MISSING = "MISSING"

    def __init__(self, timestamp, state):
        self.timestamp = timestamp
        self.state     = state

class HeartBeatMonitor(Worker):

    INTERVAL_SECS = 30

    # Make sure there is an entry for worker data in the in-memory db
    #
    # Returns a reference to the worker node
    #
    def ensure(self, system_id, worker_id):
        if not self._state.has_key(system_id):
            self._state[system_id] = {}
        if not self._state[system_id].has_key(worker_id):
            wkr = {
                    'record' : None,
                    'events' : [],
                  }
            self._state[system_id][worker_id] = wkr
        return self._state[system_id][worker_id]

    # Process a heartbeat message
    #
    def update(self, msg):
        system_id = msg.get('system_id', None)
        worker_id = msg.get('worker_id', None)

        # discard if msg format isn't valid
        if not system_id or not worker_id:
            return

        # ensure there is an entry for the worker in the db
        wkr = self.ensure(system_id, worker_id)

        # update the worker's known state
        wkr['record'] = Record(msg['system_id'],
                               msg['worker_id'],
                               msg['hostname'],
                               msg['pid'],
                               msg['timestamp'])

        # Add an event if necessary
        if not wkr['events']:
            ev = Event(msg['timestamp'], Event.ALIVE)
            wkr['events'].append(ev)
        if wkr['events'][-1].state == Event.MISSING:
            ev = Event(msg['timestamp'], Event.ALIVE)
            wkr['events'].append(ev)

    #
    # Iterate over worker records and check to see whether any have not checked-in on time
    #
    def _check_missing(self):
        tim = time.time() - HeartBeatMonitor.INTERVAL_SECS
        for system_id, workers in self._state.iteritems():
            for wkr in [wkr for wkr in workers.values() if wkr['record'].timestamp < tim]:
                log.msg("worker missing: %s" % wkr['record'].worker_id)
                last = wkr['events'][-1]
                if last.state == Event.MISSING:
                    continue
                if last.state == Event.ALIVE:
                    ev = Event(wkr['record'].timestamp, Event.MISSING)
                    wkr['events'].append(ev)

    @inlineCallbacks
    def startWorker(self):
        log.msg("Heartbeat Starting consumer")

        self._state = {}

        # Start consuming heartbeats
        yield self.consume("heartbeat.inbound", self.consume_message)

        # Start the JSON-RPC server
        root = RPCServer(self._state)
        site = server.Site(root)
        rpc_service = internet.TCPServer(7080, site)
        rpc_service.setServiceParent(service.Application("heartbeat-data-server"))
        self.addService(rpc_service)

        # Create a timer task to check for missing worker heartbeats
        self._task = LoopingCall(self._check_missing)
        done = self._task.start(self.INTERVAL_SECS, now=False)
        done.addErrback(lambda failure: log.err(failure, "timer task died"))

    def stopWorker(self):
        log.msg("HeartBeat: stopping")

    def consume_message(self, msg):
        log.msg("Received message: %s" % msg)
        self.update(msg.payload)
