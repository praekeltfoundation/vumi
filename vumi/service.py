# -*- test-case-name: vumi.tests.test_service -*-

import json
from copy import deepcopy

from twisted.python import log
from twisted.application.service import MultiService
from twisted.application.internet import TCPClient
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet import protocol, reactor
import txamqp
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient

from vumi.errors import VumiError
from vumi.message import Message
from vumi.utils import load_class_by_string, vumi_resource_path, build_web_site


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
        log.err("AmqpFactory connection failed (%s)" % (
            reason.getErrorMessage(),))
        self.worker._amqp_connection_failed()
        self.amqp_client = None
        protocol.ReconnectingClientFactory.clientConnectionFailed(
            self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        if not self.worker.running:
            # We've specifically asked for this disconnect.
            return
        log.err("AmqpFactory client connection lost (%s)" % (
            reason.getErrorMessage(),))
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

    def _declare_exchange(self, source, channel):
        # get the details for AMQP
        exchange_name = source.exchange_name
        exchange_type = source.exchange_type
        durable = source.durable
        return channel.exchange_declare(exchange=exchange_name,
                                        type=exchange_type, durable=durable)

    @inlineCallbacks
    def start_consumer(self, consumer_class, *args, **kwargs):
        channel = yield self.get_channel()

        consumer = consumer_class(channel, *args, **kwargs)
        consumer.vumi_options = self.vumi_options

        # get the details for AMQP
        exchange_name = consumer.exchange_name
        durable = consumer.durable
        queue_name = consumer.queue_name
        routing_key = consumer.routing_key

        # declare the exchange, doesn't matter if it already exists
        yield self._declare_exchange(consumer, channel)

        # declare the queue
        yield channel.queue_declare(queue=queue_name, durable=durable)
        # bind it to the exchange with the routing key
        yield channel.queue_bind(queue=queue_name, exchange=exchange_name,
                                 routing_key=routing_key)
        yield consumer.start()
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
        yield self._declare_exchange(publisher, channel)
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

    def consume(self, routing_key, callback, queue_name=None,
                exchange_name='vumi', exchange_type='direct', durable=True,
                message_class=None, paused=False, prefetch_count=None):

        # use the routing key to generate the name for the class
        # amq.routing.key -> AmqRoutingKey
        dynamic_name = self.routing_key_to_class_name(routing_key)
        class_name = "%sDynamicConsumer" % str(dynamic_name)
        kwargs = {
            'routing_key': routing_key,
            'queue_name': queue_name or routing_key,
            'exchange_name': exchange_name,
            'exchange_type': exchange_type,
            'durable': durable,
            'start_paused': paused,
            'prefetch_count': prefetch_count,
        }
        log.msg('Starting %s with %s' % (class_name, kwargs))
        klass = type(class_name, (DynamicConsumer,), kwargs)
        if message_class is not None:
            klass.message_class = message_class
        return self.start_consumer(klass, callback)

    def start_consumer(self, consumer_class, *args, **kw):
        return self._amqp_client.start_consumer(consumer_class, *args, **kw)

    def publish_to(self, routing_key,
                   exchange_name='vumi', exchange_type='direct', durable=True,
                   delivery_mode=2):
        class_name = self.routing_key_to_class_name(routing_key)
        publisher_class = type(
            "%sDynamicPublisher" % class_name, (Publisher,), {
                "routing_key": routing_key,
                "exchange_name": exchange_name,
                "exchange_type": exchange_type,
                "durable": durable,
                "delivery_mode": delivery_mode,
            })
        return self.start_publisher(publisher_class)

    def start_publisher(self, publisher_class, *args, **kw):
        return self._amqp_client.start_publisher(publisher_class, *args, **kw)

    def start_web_resources(self, resources, port, site_class=None):
        resources = dict((path, resource) for resource, path in resources)
        site_factory = build_web_site(resources, site_class=site_class)
        return reactor.listenTCP(port, site_factory)


class QueueCloseMarker(object):
    "This is a marker for closing consumer queues."


class Consumer(object):

    exchange_name = "vumi"
    exchange_type = "direct"
    durable = False

    queue_name = "queue"
    routing_key = "routing_key"

    message_class = Message
    start_paused = False
    prefetch_count = None

    def __init__(self, channel):
        self.channel = channel
        self._notify_paused_and_quiet = []
        self.keep_consuming = False
        self._testing = hasattr(self.channel, 'message_processed')
        self.queue = None
        self._consumer_tag = None

    @inlineCallbacks
    def start(self):
        self._in_progress = 0
        self.keep_consuming = True
        self.paused = self.start_paused
        self._unpause_d = None
        if self.prefetch_count is not None:
            yield self.channel.basic_qos(0, self.prefetch_count, False)
        if not self.paused:
            yield self.unpause()
        returnValue(self)

    @inlineCallbacks
    def _read_messages(self):
        try:
            while self.keep_consuming:
                message = yield self.queue.get()
                if isinstance(message, QueueCloseMarker):
                    break
                if self.paused:
                    yield self._unpause_d
                yield self.consume(message)
        except txamqp.queue.Closed as e:
            log.err("Queue has closed", e)
        except Exception:
            # Log this explicitly instead of waiting for the deferred to be
            # garbage-collected, because that might only happen later on pypy.
            log.err()

    @inlineCallbacks
    def _channel_consume(self):
        if self._consumer_tag is not None:
            raise RuntimeError("Consumer already registered.")
        reply = yield self.channel.basic_consume(queue=self.queue_name)
        self._consumer_tag = reply.consumer_tag
        self.queue = yield self.channel.client.queue(self._consumer_tag)
        self.keep_consuming = True
        self._read_messages()

    @inlineCallbacks
    def pause(self):
        self.paused = True
        if self._unpause_d is None:
            self._unpause_d = Deferred()
        yield self.notify_paused_and_quiet()

    def unpause(self):
        self.paused = False
        d, self._unpause_d = self._unpause_d, None
        if d is not None:
            d.callback(None)
        if self._consumer_tag is None:
            return self._channel_consume()

    def notify_paused_and_quiet(self):
        d = Deferred()
        self._notify_paused_and_quiet.append(d)
        self._check_notify()
        return d

    def _check_notify(self):
        if self.paused and not self._in_progress:
            while self._notify_paused_and_quiet:
                self._notify_paused_and_quiet.pop(0).callback(None)

    @inlineCallbacks
    def consume(self, message):
        self._in_progress += 1
        try:
            result = yield self.consume_message(
                self.message_class.from_json(message.content.body))
        finally:
            # If we get an exception here the consumer's already pretty much
            # broken, but we still decrement the _in_progress counter so we
            # don't wait forever for it during shutdown.
            self._in_progress -= 1
            if self._testing:
                self.channel.message_processed()
        if result is not False:
            yield self.channel.basic_ack(message.delivery_tag, False)
        else:
            log.msg('Received %s as a return value consume_message. '
                    'Not acknowledging AMQ message' % result)
        self._check_notify()

    def consume_message(self, message):
        """helper method, override in implementation"""
        log.msg("Received message: %s" % message)

    @inlineCallbacks
    def stop(self):
        log.msg("Consumer stopping...")
        self.keep_consuming = False
        yield self.pause()
        # This actually closes the channel on the server
        yield self.channel.channel_close()
        # This just marks the channel as closed on the client
        self.channel.close(None)
        returnValue(self.keep_consuming)


class DynamicConsumer(Consumer):
    def __init__(self, channel, callback):
        super(DynamicConsumer, self).__init__(channel)
        self.callback = callback

    def consume_message(self, message):
        return self.callback(message)


class RoutingKeyError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Publisher(object):
    exchange_name = "vumi"
    exchange_type = "direct"
    routing_key = "routing_key"
    durable = False
    auto_delete = False
    delivery_mode = 2  # save to disk

    def start(self, channel):
        log.msg("Started the publisher")
        self.channel = channel
        self.bound_routing_keys = {}

        # There's probably a better way to do this.
        if not hasattr(self, 'vumi_options'):
            self.vumi_options = {}

    def check_routing_key(self, routing_key):
        if(routing_key != routing_key.lower()):
            raise RoutingKeyError("The routing_key: %s is not all lower case!"
                                  % (routing_key))

    @inlineCallbacks
    def publish(self, message, **kwargs):
        exchange_name = kwargs.get('exchange_name') or self.exchange_name
        routing_key = kwargs.get('routing_key') or self.routing_key
        self.check_routing_key(routing_key)
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
        amq_message['delivery mode'] = kwargs.pop(
            'delivery_mode', self.delivery_mode)
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
