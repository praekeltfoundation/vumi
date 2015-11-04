# -*- test-case-name: vumi.tests.test_service -*-

import json
import warnings
from copy import deepcopy

from pika import BasicProperties
import pika.exceptions
from twisted.python import log
from twisted.application.service import MultiService
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet.endpoints import clientFromString
from twisted.internet import reactor

from vumi.amqp_service import AMQPClientService
from vumi.errors import VumiError
from vumi.message import Message
from vumi.utils import load_class_by_string, build_web_site


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

    @inlineCallbacks
    def publish_to(self, routing_key):
        channel = yield self._amqp_client.get_channel()
        publisher = DynamicPublisher(channel, routing_key)
        yield self._amqp_client._declare_exchange(publisher, channel)
        # return the publisher
        returnValue(publisher)

    def start_publisher(self, publisher_class, *args, **kw):
        return self._amqp_client.start_publisher(publisher_class, *args, **kw)

    def start_web_resources(self, resources, port, site_class=None):
        resources = dict((path, resource) for resource, path in resources)
        site_factory = build_web_site(resources, site_class=site_class)
        return reactor.listenTCP(port, site_factory)


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
        self._fake_channel = getattr(self.channel, '_fake_channel', None)
        self._notify_paused_and_quiet = []
        self.keep_consuming = False
        self.queue = None
        self._consumer_tag = None

    @inlineCallbacks
    def start(self):
        self._in_progress = 0
        self.keep_consuming = True
        self.paused = self.start_paused
        self._unpause_d = None
        if self.prefetch_count is not None:
            yield self.channel.basic_qos(
                prefetch_size=0, prefetch_count=self.prefetch_count,
                all_channels=False)
        if not self.paused:
            yield self.unpause()
        returnValue(self)

    @inlineCallbacks
    def _read_messages(self):
        try:
            while self.keep_consuming:
                message = yield self.queue.get()
                if self.paused:
                    yield self._unpause_d
                delivery_tag = message[1].delivery_tag
                body = message[3]
                yield self.consume(delivery_tag, body)
        except pika.exceptions.ChannelClosed:
            log.msg("Queue has closed")
        except Exception:
            # Log this explicitly instead of waiting for the deferred to be
            # garbage-collected, because that might only happen later on pypy.
            log.err()

    @inlineCallbacks
    def _channel_consume(self):
        if self._consumer_tag is not None:
            raise RuntimeError("Consumer already registered.")
        reply = yield self.channel.basic_consume(queue=self.queue_name)
        self.queue, self._consumer_tag = reply
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
    def consume(self, delivery_tag, body):
        self._in_progress += 1
        try:
            result = yield self.consume_message(
                self.message_class.from_json(body))
        finally:
            # If we get an exception here the consumer's already pretty much
            # broken, but we still decrement the _in_progress counter so we
            # don't wait forever for it during shutdown.
            self._in_progress -= 1
            if self._fake_channel is not None:
                self._fake_channel.message_processed()
        if result is not False:
            yield self.channel.basic_ack(delivery_tag, False)
        else:
            log.msg('Received %s as a return value consume_message. '
                    'Not acknowledging AMQ message' % result)
        self._check_notify()

    def consume_message(self, message):
        """helper method, override in implementation"""
        log.msg("Received message: %s" % message)

    @inlineCallbacks
    def stop(self):
        if self._consumer_tag is None:
            # We're not running.
            self.keep_consuming = False
            returnValue(self.keep_consuming)
        log.msg("Consumer stopping...")
        self.keep_consuming = False
        yield self.pause()
        yield self.channel.close()
        self._consumer_tag = None
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


class _Publisher(object):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = False
    auto_delete = False
    delivery_mode = 2  # save to disk

    def check_routing_key(self, routing_key):
        if routing_key != routing_key.lower():
            raise RoutingKeyError(
                "The routing_key: %s is not all lower case!" % (routing_key,))


class Publisher(_Publisher):
    """
    An old-style publisher to subclass for special-purpose publishers.
    This is deprecated in favour of using :meth:`Worker.publish_to`, although
    it will stay around for a while.
    """

    routing_key = "routing_key"

    def start(self, channel):
        warnings.warn(
            "Subclassing the Publisher class is deprecated. Please use"
            " Worker.publish_to() instead.", category=DeprecationWarning)
        log.msg("Started the publisher")
        self.channel = channel
        self.bound_routing_keys = {}

    @inlineCallbacks
    def _publish(self, message, routing_key=None):
        if routing_key is None:
            routing_key = self.routing_key
            self.check_routing_key(routing_key)
        properties = BasicProperties(delivery_mode=self.delivery_mode)
        yield self.channel.basic_publish(
            exchange=self.exchange_name, routing_key=routing_key,
            body=message, properties=properties)

    def publish_message(self, message, routing_key=None):
        d = self.publish_raw(message.to_json(), routing_key=routing_key)
        d.addCallback(lambda r: message)
        return d

    def publish_json(self, data, routing_key=None):
        """helper method"""
        return self.publish_raw(json.dumps(data, cls=json.JSONEncoder),
                                routing_key=routing_key)

    def publish_raw(self, data, routing_key=None):
        return self._publish(data, routing_key=routing_key)


class DynamicPublisher(_Publisher):
    """
    A single-routing-key publisher.
    """

    def __init__(self, channel, routing_key):
        self.channel = channel
        self.check_routing_key(routing_key)
        self.routing_key = routing_key

    def publish_message(self, message):
        d = self.publish_raw(message.to_json())
        d.addCallback(lambda r: message)
        return d

    def publish_json(self, data):
        return self.publish_raw(json.dumps(data, cls=json.JSONEncoder))

    def publish_raw(self, data):
        return self._publish(data)

    def _publish(self, message):
        properties = BasicProperties(delivery_mode=self.delivery_mode)
        return self.channel.basic_publish(
            exchange=self.exchange_name, routing_key=self.routing_key,
            body=message, properties=properties)


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

    def _get_endpoint(self):
        return clientFromString(
            reactor, "tcp://%(hostname)s:%(port)s" % self.options)

    def _get_service(self, endpoint):
        return AMQPClientService(endpoint)

    def _connect(self, worker, timeout, bindAddress):
        service = self._get_service(self._get_endpoint())
        service.await_connected().addCallback(worker._amqp_connected)
        service.setServiceParent(worker)
