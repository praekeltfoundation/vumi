from twisted.python import log, usage
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet import protocol, reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient
from vumi.errors import VumiError
from vumi.message import Message
from vumi.webapp.api import utils
import txamqp
import json
import vumi.options


class Options(usage.Options):
    """
    Default options for all workers created
    """
    optParameters = [
        ["hostname", None, "127.0.0.1", "AMQP broker"],
        ["port", None, 5672, "AMQP port", int],
        ["username", None, "vumi", "AMQP username"],
        ["password", None, "vumi", "AMQP password"],
        ["vhost", None, "/develop", "AMQP virtual host"],
        ["specfile", None, "config/amqp-spec-0-8.xml", "AMQP spec file"],
    ]


class Worker(AMQClient):
    """
    The Worker is responsible for starting consumers & publishers
    as needed.
    """
    @inlineCallbacks
    def connectionMade(self):
        AMQClient.connectionMade(self)
        yield self.authenticate(self.factory.username, self.factory.password)
        # authentication was successful
        log.msg("Got an authenticated connection")
        yield self.startWorker()

    @inlineCallbacks
    def startWorker(self):
        # I hate camelCasing method but since Twisted has it as a
        # standard I voting to stick with it
        raise VumiError("You need to subclass Worker and its "
                        "startWorker method")

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

    def routing_key_to_class_name(self, routing_key):
        return ''.join(map(lambda s: s.capitalize(), routing_key.split('.')))

    def consume(self, routing_key, callback, queue_name=None,
                exchange_name='vumi', exchange_type='direct', durable=True):

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
        }
        log.msg('Staring %s with %s' % (class_name, kwargs))
        klass = type(class_name, (DynamicConsumer,), kwargs)
        return self.start_consumer(klass, callback)

    @inlineCallbacks
    def start_consumer(self, klass, *args, **kwargs):
        channel = yield self.get_channel()
        consumer = klass(*args, **kwargs)

        # get the details for AMQP
        exchange_name = consumer.exchange_name
        exchange_type = consumer.exchange_type
        durable = consumer.durable
        queue_name = consumer.queue_name
        routing_key = consumer.routing_key

        # declare the exchange, doesn't matter if it already exists
        yield channel.exchange_declare(exchange=exchange_name,
                                        type=exchange_type, durable=durable)

        # declare the queue
        yield channel.queue_declare(queue=queue_name, durable=durable)
        # bind it to the exchange with the routing key
        yield channel.queue_bind(queue=queue_name, exchange=exchange_name,
                                 routing_key=routing_key)
        # register the consumer
        reply = yield channel.basic_consume(queue=queue_name)
        queue = yield self.queue(reply.consumer_tag)
        # start consuming! nom nom nom
        consumer.start(channel, queue)
        # return the newly created & consuming consumer
        returnValue(consumer)

    def publish_to(self, routing_key, exchange_name='vumi',
                   exchange_type='direct', delivery_mode=2):
        class_name = self.routing_key_to_class_name(routing_key)
        publisher_class = type("%sDynamicPublisher" % class_name, (Publisher,),
            {
                "routing_key": routing_key,
                "exchange_name": exchange_name,
                "exchange_type": exchange_type,
                "delivery_mode": delivery_mode,
            })
        return self.start_publisher(publisher_class)

    @inlineCallbacks
    def start_publisher(self, klass, *args, **kwargs):
        # much more braindead than start_consumer
        # get a channel
        channel = yield self.get_channel()
        # start the publisher
        publisher = klass(*args, **kwargs)
        # start!
        yield publisher.start(channel)
        # return the publisher
        returnValue(publisher)

    @inlineCallbacks
    def start_web_resources(self, resources, port):
        # start the HTTP server for receiving the receipts
        root = Resource()
        # sort by ascending path length to make sure we create
        # resources lower down in the path earlier
        resource = sorted(resources, key=lambda r: len(r[1]))
        for resource, path in resources:
            request_path = filter(None, path.split('/'))
            nodes, leaf = request_path[0:-1], request_path[-1]

            def create_node(node, path):
                if path in node.children:
                    return node.children.get(path)
                else:
                    new_node = Resource()
                    node.putChild(path, new_node)
                    return new_node

            parent = reduce(create_node, nodes, root)
            parent.putChild(leaf, resource)

        site_factory = Site(root)
        yield reactor.listenTCP(port, site_factory)
        returnValue(root)


class Consumer(object):

    exchange_name = "vumi"
    exchange_type = "direct"
    durable = False

    queue_name = "queue"
    routing_key = "routing_key"

    @inlineCallbacks
    def start(self, channel, queue):
        self.channel = channel
        self.queue = queue
        self.keep_consuming = True

        @inlineCallbacks
        def read_messages():
            log.msg("Consumer starting...")
            try:
                while self.keep_consuming:
                    message = yield self.queue.get()
                    yield self.consume(message)
            except txamqp.queue.Closed, e:
                log.err("Queue has closed", e)

        read_messages()
        yield None
        returnValue(self)

    @inlineCallbacks
    def consume(self, message):
        result = yield self.consume_message(Message.from_json(
                                            message.content.body))
        if result is not False:
            returnValue(self.ack(message))
        else:
            log.msg('Received %s as a return value consume_message. '
                    'Not acknowledging AMQ message' % result)

    def consume_message(self, message):
        """helper method, override in implementation"""
        log.msg("Received message: %s" % message)

    def ack(self, message):
        self.channel.basic_ack(message.delivery_tag, True)

    @inlineCallbacks
    def stop(self):
        self.keep_consuming = False
        # This just marks the channel as closed on the client
        #self.channel.close(None)
        # This actually closes the channel on the server
        yield self.channel.channel_close()
        self.channel.close()
        returnValue(self.keep_consuming)


class DynamicConsumer(Consumer):
    def __init__(self, callback):
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
        self.vumi_options = vumi.options.get_all()
        self.bound_routing_keys = {}

    def list_bindings(self):
        try:
            # Note utils.callback() does a POST not a GET
            # which may lead to errors if the RabbitMQ Management REST api
            # changes
            url, resp = utils.callback(
                "http://%s:%s@localhost:55672/api/bindings" % (
                    self.vumi_options['username'],
                    self.vumi_options['password']), [])
            bindings = json.loads(resp)
            bound_routing_keys = {}
            for b in bindings:
                if (b['vhost'] == self.vumi_options['vhost'] and
                    b['source'] == self.exchange_name):
                    bound_routing_keys[b['routing_key']] = \
                            bound_routing_keys.get(b['routing_key'], []) + \
                            [b['destination']]
        except:
            bound_routing_keys = {"bindings": "undetected"}
        return bound_routing_keys

    def routing_key_is_bound(self, key):
        # Don't check for bound routing keys on RPC reply exchanges
        # The one-use queues are changing too frequently to cache efficiently,
        # too many http calls to RabbitMQ Management will be required,
        # and the auto-generated queues & routing_keys are unlikley to
        # result in errors where routing keys are unbound
        if self.exchange_name[-4:].lower() == '_rpc':
            return True
        if (len(self.bound_routing_keys) == 1 and
            self.bound_routing_keys.get("bindings") == "undetected"):
            log.msg("No bindings detected, is the RabbitMQ Management plugin"
                    " installed?")
            return True
        if key in self.bound_routing_keys.keys():
            return True
        self.bound_routing_keys = self.list_bindings()
        if (len(self.bound_routing_keys) == 1 and
            self.bound_routing_keys.get("bindings") == "undetected"):
            log.msg("No bindings detected, is the RabbitMQ Management plugin"
                    " installed?")
            return True
        return key in self.bound_routing_keys.keys()

    def check_routing_key(self, routing_key, require_bind):
        if(routing_key != routing_key.lower()):
            raise RoutingKeyError("The routing_key: %s is not all lower case!"
                                  % (routing_key))
        if not self.routing_key_is_bound(routing_key):
            raise RoutingKeyError("The routing_key: %s is not bound to any"
                                  " queues in vhost: %s  exchange: %s" % (
                                  routing_key, self.vhost, self.exchange_name))

    def publish(self, message, **kwargs):
        exchange_name = kwargs.get('exchange_name') or self.exchange_name
        routing_key = kwargs.get('routing_key') or self.routing_key
        require_bind = kwargs.get('require_bind')
        self.check_routing_key(routing_key, require_bind)
        self.channel.basic_publish(exchange=exchange_name, content=message,
                                   routing_key=routing_key)

    def publish_message(self, message, **kwargs):
        amq_message = Content(message.to_json())
        amq_message['delivery mode'] = kwargs.pop('delivery_mode',
                self.delivery_mode)
        return self.publish(amq_message, **kwargs)

    def publish_json(self, data, **kwargs):
        """helper method"""
        message = Content(json.dumps(data, cls=json.JSONEncoder))
        message['delivery mode'] = kwargs.pop('delivery_mode',
                                              self.delivery_mode)
        return self.publish(message, **kwargs)


class AmqpFactory(protocol.ReconnectingClientFactory):

    def __init__(self, specfile, vhost, username, password, worker_class,
                 **options):
        self.username = username
        self.password = password
        self.vhost = vhost
        self.spec = txamqp.spec.load(specfile)
        self.delegate = TwistedDelegate()
        self.worker_class = worker_class
        self.options = options

    def buildProtocol(self, addr):
        worker = self.worker_class(self.delegate, self.vhost, self.spec)
        worker.factory = self
        worker.config = self.options.get('config', {})
        self.worker = worker
        self.resetDelay()
        return worker

    def clientConnectionFailed(self, connector, reason):
        log.err("Connection failed.", reason)
        self.worker.stopWorker()
        protocol.ReconnectingClientFactory.clientConnectionLost(self,
                connector, reason)

    def clientConnectionLost(self, connector, reason):
        log.err("Client connection lost.", reason)
        self.worker.stopWorker()
        protocol.ReconnectingClientFactory.clientConnectionFailed(self,
                connector, reason)


class WorkerCreator(object):
    """
    Creates workers
    """

    def __init__(self, worker_class, *args, **kwargs):
        self.args = args
        self.options = kwargs
        self.kwargs = kwargs
        # FIXME: shouldn't be needed
        self.kwargs.update({
            'worker_class': worker_class,
        })

    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        factory = AmqpFactory(*self.args, **self.kwargs)
        reactor.connectTCP(host, port, factory, timeout=timeout,
                           bindAddress=bindAddress)
