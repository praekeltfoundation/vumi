# -*- test-case-name: vumi.tests.test_service -*-

import json
from copy import deepcopy

from twisted.python import log
from twisted.application.service import MultiService
from twisted.application.internet import TCPClient
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.task import LoopingCall
from twisted.internet import protocol, reactor
import txamqp
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient

from vumi.errors import VumiError
from vumi.message import Message
from vumi.utils import (load_class_by_string, vumi_resource_path, http_request,
                        basic_auth_string, build_web_site)


SPECS = {}


def get_spec(specfile):
    """
    Cache the generated part of txamqp, because generating it is expensive.

    This is important for tests, which create lots of txamqp clients,
    and therefore generate lots of specs. Just doing this results in a
    decidedly happy test run time reduction.
    """
    if specfile not in SPECS:
        SPECS[specfile] = txamqp.spec.load(specfile)
    return SPECS[specfile]


class AmqpFactory(protocol.ReconnectingClientFactory):

    def __init__(self, worker):
        self.options = worker.options
        self.config = worker.config
        self.spec = get_spec(vumi_resource_path(worker.options['specfile']))
        self.delegate = TwistedDelegate()
        self.worker = worker
        self.amqp_client = None

    def buildProtocol(self, addr):
        self.amqp_client = WorkerAMQClient(
            self.delegate, self.options['vhost'],
            self.spec, self.options.get('heartbeat', 0))
        self.amqp_client.factory = self
        self.amqp_client.vumi_options = self.options
        self.amqp_client.connected_callback = self.worker._amqp_connected
        self.resetDelay()
        return self.amqp_client

    def clientConnectionFailed(self, connector, reason):
        log.err("Connection failed: %r" % (reason,))
        self.worker._amqp_connection_failed()
        self.amqp_client = None
        protocol.ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        if not self.worker.running:
            # We've specifically asked for this disconnect.
            return
        log.err("Client connection lost: %r" % (reason,))
        self.worker._amqp_connection_failed()
        self.amqp_client = None
        protocol.ReconnectingClientFactory.clientConnectionLost(
            self, connector, reason)


class WorkerAMQClient(AMQClient):
    @inlineCallbacks
    def connectionMade(self):
        AMQClient.connectionMade(self)
        yield self.authenticate(self.vumi_options['username'],
                                self.vumi_options['password'])
        # authentication was successful
        log.msg("Got an authenticated connection")
        yield self.connected_callback(self)

    @inlineCallbacks
    def get_channel(self, channel_id=None):
        """If channel_id is None a new channel is created"""
        if channel_id:
            channel = self.channels[channel_id]
        else:
            channel_id = self.get_new_channel_id()
            channel = yield self.channel(channel_id)
            yield channel.channel_open()
            self.channels[channel_id] = channel
        returnValue(channel)

    def get_new_channel_id(self):
        """
        AMQClient keeps track of channels in a dictionary. The
        channel ids are the keys, get the highest number and up it
        or just return zero for the first channel
        """
        return (max(self.channels) + 1) if self.channels else 0

    def _declare_exchange(self, exchange, channel):
        return channel.exchange_declare(exchange=exchange.name,
                                        type=exchange.exchange_type,
                                        durable=exchange.durable)

    @inlineCallbacks
    def start_consumer(self, consumer_class, *args, **kwargs):
        channel = yield self.get_channel()
        consumer = consumer_class(*args, **kwargs)
        if consumer.start_paused:
            channel.channel_flow(active=False)
        consumer.vumi_options = self.vumi_options

        # get the details for AMQP
        exchange = consumer.exchange
        queue_name = consumer.queue_name
        routing_key = consumer.routing_key
        arguments = {}

        # declare the exchange, doesn't matter if it already exists
        yield self._declare_exchange(consumer.exchange, channel)
        if consumer.dead_letter_exchange is not None:
            yield self._declare_exchange(consumer.dead_letter_exchange,
                                         channel)
            arguments["x-dead-letter-exchange"] = (
                consumer.dead_letter_exchange.name)

        # declare the queue
        yield channel.queue_declare(queue=queue_name, durable=exchange.durable,
                                    arguments=arguments)
        # bind it to the exchange with the routing key
        yield channel.queue_bind(queue=queue_name, exchange=exchange.name,
                                 routing_key=routing_key)
        # register the consumer
        reply = yield channel.basic_consume(queue=queue_name)
        queue = yield self.queue(reply.consumer_tag)
        # start consuming! nom nom nom
        consumer.start(channel, queue)
        # return the newly created & consuming consumer
        returnValue(consumer)

    @inlineCallbacks
    def start_publisher(self, publisher_class, *args, **kwargs):
        # much more braindead than start_consumer
        # get a channel
        channel = yield self.get_channel()
        # start the publisher
        publisher = publisher_class(*args, **kwargs)
        publisher.vumi_options = self.vumi_options
        # declare the exchange, doesn't matter if it already exists
        yield self._declare_exchange(publisher.exchange, channel)
        # start!
        yield publisher.start(channel)
        # return the publisher
        returnValue(publisher)


class Worker(MultiService, object):
    """
    The Worker is responsible for starting consumers & publishers
    as needed.
    """

    def __init__(self, options, config=None):
        super(Worker, self).__init__()
        self.options = options
        if config is None:
            config = {}
        self.config = config
        self._amqp_client = None

    def _amqp_connected(self, amqp_client):
        self._amqp_client = amqp_client
        return self.startWorker()

    def _amqp_connection_failed(self):
        pass

    def _amqp_connection_lost(self):
        self._amqp_client = None

    def startWorker(self):
        # I hate camelCasing method but since Twisted has it as a
        # standard I voting to stick with it
        raise VumiError("You need to subclass Worker and its "
                        "startWorker method")

    def stopWorker(self):
        pass

    @inlineCallbacks
    def stopService(self):
        if self.running:
            yield self.stopWorker()
        yield super(Worker, self).stopService()

    def routing_key_to_class_name(self, routing_key):
        return ''.join(map(lambda s: s.capitalize(), routing_key.split('.')))

    def consume(self, routing_key, callback, errback=None, queue_name=None,
                exchange_name='vumi', exchange_type='direct', durable=True,
                message_class=None, paused=False):

        # use the routing key to generate the name for the class
        # amq.routing.key -> AmqRoutingKey
        dynamic_name = self.routing_key_to_class_name(routing_key)
        class_name = "%sDynamicConsumer" % dynamic_name
        kwargs = {
            'routing_key': routing_key,
            'queue_name': queue_name or routing_key,
            'exchange': Exchange(exchange_name, exchange_type=exchange_type,
                                 durable=durable),
            'start_paused': paused,
        }
        log.msg('Starting %s with %s' % (class_name, kwargs))
        klass = type(class_name, (DynamicConsumer,), kwargs)
        if message_class is not None:
            klass.message_class = message_class
        return self.start_consumer(klass, callback, errback=errback)

    def start_consumer(self, consumer_class, *args, **kw):
        return self._amqp_client.start_consumer(consumer_class, *args, **kw)

    def publish_to(self, routing_key,
                   exchange_name='vumi', exchange_type='direct', durable=True,
                   delivery_mode=2):
        dynamic_name = self.routing_key_to_class_name(routing_key)
        class_name = "%sDynamicPublisher" % dynamic_name
        kwargs = {
            "routing_key": routing_key,
            "exchange": Exchange(exchange_name, exchange_type=exchange_type,
                                 durable=durable),
            "delivery_mode": delivery_mode,
        }
        log.msg('Starting %s with %s' % (class_name, kwargs))
        publisher_class = type(class_name, (Publisher,), kwargs)
        return self.start_publisher(publisher_class)

    def start_publisher(self, publisher_class, *args, **kw):
        return self._amqp_client.start_publisher(publisher_class, *args, **kw)

    def start_web_resources(self, resources, port, site_class=None):
        resources = dict((path, resource) for resource, path in resources)
        site_factory = build_web_site(resources, site_class=site_class)
        return reactor.listenTCP(port, site_factory)


class QueueCloseMarker(object):
    "This is a marker for closing consumer queues."


class Exchange(object):
    def __init__(self, name, exchange_type, durable):
        self.name = name
        self.exchange_type = exchange_type
        self.durable = durable


class Consumer(object):

    exchange = Exchange(name="vumi", exchange_type="direct", durable=False)

    # set dead_letter_exchange to None to disable
    dead_letter_exchange = Exchange(name="vumi.dead_letter",
                                    exchange_type="direct", durable=True)

    queue_name = "queue"
    routing_key = "routing_key"

    message_class = Message
    start_paused = False

    _consume_loop = None

    def start(self, channel, queue):
        log.msg("Consumer starting...")
        self.channel = channel
        self.queue = queue
        self.paused = self.start_paused
        self._consume_loop = LoopingCall(self._consume)
        self._consume_done = self._consume_loop.start(0)

    @inlineCallbacks
    def _consume(self):
        message = yield self.queue.get()
        if isinstance(message, QueueCloseMarker):
            log.msg("Queue closed.")
            self._consume_loop.stop()
            return
        try:
            yield self.consume_message(
                self.message_class.from_json(message.content.body))
            self.ack(message)
        except Exception as err:
            log.err()
            yield self.consume_error(err, message)
            self.nack(message)

    def consume_message(self, message):
        """Fallback consume method.

        Logs the message at the `info` logging level.

        Should be overridden by subclasses.
        """
        log.msg("Received message: %s" % (message,))

    def consume_error(self, err, message):
        """Fallback error handling method.

        Logs the message body at the `info` logging level.

        Should be overridden by subclasses.
        """
        log.msg("Logging failed message: %r" % (message.content.body,))

    def pause(self):
        self.paused = True
        return self.channel.channel_flow(active=False)

    def unpause(self):
        self.paused = False
        return self.channel.channel_flow(active=True)

    def ack(self, message, multiple=False):
        """Acknowledge a message as processed."""
        self.channel.basic_ack(message.delivery_tag, multiple=multiple)

    def nack(self, message, multiple=False, requeue=True):
        """Reject a message as unprocessed."""
        self.channel.basic_nack(message.delivery_tag, multiple=multiple,
                                requeue=requeue)

    def running(self):
        """Return True if the consumer loop is running and False otherwise."""
        return self._consume_done is not None and not self._consume_done.called

    @inlineCallbacks
    def stop(self):
        log.msg("Consumer stopping...")
        # This actually closes the channel on the server
        yield self.channel.channel_close()
        # This just marks the channel as closed on the client
        self.channel.close(None)
        # This waits for the client to consume its current messages
        if self._consume_loop is not None:
            self.queue.put(QueueCloseMarker())
            yield self._consume_done


class DynamicConsumer(Consumer):
    def __init__(self, callback, errback=None):
        self.callback = callback
        self.errback = (errback if errback is not None
                        else self._default_errback)

    def consume_message(self, message):
        return self.callback(message)

    def _default_errback(self, err, message):
        log.msg("Logging failed message: %r" % (message.content.body,))

    def consume_error(self, err, message):
        return self.errback(err, message)


class RoutingKeyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Publisher(object):

    exchange = Exchange(name="vumi", exchange_type="direct", durable=False)

    routing_key = "routing_key"
    require_bind = True
    auto_delete = False
    delivery_mode = 2  # save to disk

    def start(self, channel):
        log.msg("Started the publisher")
        self.channel = channel
        self.bound_routing_keys = {}

        # There's probably a better way to do this.
        if not hasattr(self, 'vumi_options'):
            self.vumi_options = {}

    @inlineCallbacks
    def list_bindings(self):
        try:
            # Note utils.callback() does a POST not a GET
            # which may lead to errors if the RabbitMQ Management REST api
            # changes
            resp = yield http_request(
                "http://localhost:55672/api/bindings", headers={
                    'Authorization': basic_auth_string(
                        self.vumi_options['username'],
                        self.vumi_options['password']),
                    })
            bindings = json.loads(resp)
            bound_routing_keys = {}
            for b in bindings:
                if (b['vhost'] == self.vumi_options['vhost'] and
                        b['source'] == self.exchange.name):
                    bound_routing_keys[b['routing_key']] = \
                            bound_routing_keys.get(b['routing_key'], []) + \
                            [b['destination']]
        except:
            bound_routing_keys = {"bindings": "undetected"}
        returnValue(bound_routing_keys)

    @inlineCallbacks
    def routing_key_is_bound(self, key):
        # Don't check for bound routing keys on RPC reply exchanges
        # The one-use queues are changing too frequently to cache efficiently,
        # too many http calls to RabbitMQ Management will be required,
        # and the auto-generated queues & routing_keys are unlikley to
        # result in errors where routing keys are unbound
        if self.exchange.name[-4:].lower() == '_rpc':
            returnValue(True)
        if (len(self.bound_routing_keys) == 1 and
                self.bound_routing_keys.get("bindings") == "undetected"):
            # The following is very noisy in the logs:
            # log.msg("No bindings detected, is the RabbitMQ Management plugin"
            #         " installed?")
            returnValue(True)
        if key in self.bound_routing_keys.keys():
            returnValue(True)
        self.bound_routing_keys = yield self.list_bindings()
        if (len(self.bound_routing_keys) == 1 and
                self.bound_routing_keys.get("bindings") == "undetected"):
            # The following is very noisy in the logs:
            # log.msg("No bindings detected, is the RabbitMQ Management plugin"
            #         " installed?")
            returnValue(True)
        returnValue(key in self.bound_routing_keys.keys())

    @inlineCallbacks
    def check_routing_key(self, routing_key, require_bind):
        if(routing_key != routing_key.lower()):
            raise RoutingKeyError("The routing_key: %s is not all lower case!"
                                  % (routing_key))
        if not require_bind:
            return
        is_bound = yield self.routing_key_is_bound(routing_key)
        if not is_bound:
            raise RoutingKeyError("The routing_key: %s is not bound to any"
                                  " queues in vhost: %s  exchange: %s" % (
                                  routing_key, self.vumi_options['vhost'],
                                  self.exchange.name))

    @inlineCallbacks
    def publish(self, message, **kwargs):
        exchange_name = kwargs.get('exchange_name') or self.exchange.name
        routing_key = kwargs.get('routing_key') or self.routing_key
        require_bind = kwargs.get('require_bind', self.require_bind)
        yield self.check_routing_key(routing_key, require_bind)
        yield self.channel.basic_publish(exchange=exchange_name,
                                         content=message,
                                         routing_key=routing_key)

    def publish_message(self, message, **kwargs):
        d = self.publish_raw(message.to_json(), **kwargs)
        d.addCallback(lambda r: message)
        return d

    def publish_json(self, data, **kw):
        """helper method"""
        return self.publish_raw(json.dumps(data, cls=json.JSONEncoder), **kw)

    def publish_raw(self, data, **kwargs):
        amq_message = Content(data)
        amq_message['delivery mode'] = kwargs.pop('delivery_mode',
                self.delivery_mode)
        return self.publish(amq_message, **kwargs)


class WorkerCreator(object):
    """
    Creates workers
    """

    def __init__(self, vumi_options):
        self.options = vumi_options

    def create_worker(self, worker_class, config, timeout=30,
                      bindAddress=None):
        """
        Create a worker factory, connect to AMQP and return the factory.

        Return value is the AmqpFactory instance containing the worker.
        """
        return self.create_worker_by_class(
            load_class_by_string(worker_class), config, timeout=timeout,
            bindAddress=bindAddress)

    def create_worker_by_class(self, worker_class, config, timeout=30,
                               bindAddress=None):
        worker = worker_class(deepcopy(self.options), config)
        self._connect(worker, timeout=timeout, bindAddress=bindAddress)
        return worker

    def _connect(self, worker, timeout, bindAddress):
        service = TCPClient(self.options['hostname'], self.options['port'],
                            AmqpFactory(worker), timeout, bindAddress)
        service.setServiceParent(worker)
