from twisted.python import log, usage, failure
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from twisted.internet import protocol, reactor
from twisted.web.server import Site
from twisted.web.resource import Resource
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.protocol import AMQClient
from vumi.errors import VumiError
from vumi.message import Message
import txamqp
import json, datetime, sys

class Options(usage.Options):
    """
    Default options for all workers created
    """
    optParameters = [
        ["hostname", None, "127.0.0.1", "AMQP broker"],
        ["port", None, 5672, "AMQP port", int],
        ["username", None, "vumi", "AMQP username"],
        ["password", None, "vumi", "AMQP password"],
        ["vhost", None, "/vumi", "AMQP virtual host"],
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
        raise VumiError, "You need to subclass Worker and its " \
                             "startWorker method"
    
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
        yield self.consume_message(Message.from_json(message.content.body))
        returnValue(self.ack(message))
    
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
        d = self.channel.channel_close()
        d.addCallback(self.channel.close)
        returnValue(self.keep_consuming)


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
    delivery_mode = 2 # save to disk
    
    def start(self, channel):
        log.msg("Started the publisher")
        self.channel = channel

    def check_routing_key(self, routing_key):
        if(routing_key != routing_key.lower()):
            raise RoutingKeyError("The routing_key: %s is not all lower case!" % (routing_key))
        # TODO More routing_key error checks to follow
    
    def publish(self, message, **kwargs):
        exchange_name = kwargs.get('exchange_name') or self.exchange_name
        routing_key = kwargs.get('routing_key') or self.routing_key
        self.check_routing_key(routing_key)
        self.channel.basic_publish(exchange=exchange_name, 
                                        content=message, 
                                        routing_key=routing_key)
    
    def publish_message(self, message, **kwargs):
        amq_message = Content(message.to_json())
        amq_message['delivery mode'] = kwargs.pop('delivery_mode',
                self.delivery_mode)
        return self.publish(amq_message, **kwargs)

    def publish_json(self, data, **kwargs):
        """helper method"""
        message = Content(json.dumps(data, cls=JSONEncoder))
        message['delivery mode'] = kwargs.pop('delivery_mode', self.delivery_mode)
        return self.publish(message, **kwargs)


class AmqpFactory(protocol.ReconnectingClientFactory):
    
    def __init__(self, specfile, vhost, username, password, worker_class, **options):
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
        protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)
    
    def clientConnectionLost(self, connector, reason):
        log.err("Client connection lost.", reason)
        self.worker.stopWorker()
        protocol.ReconnectingClientFactory.clientConnectionFailed(self, connector, reason)
    

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
            'worker_class': worker_class
        })
    
    def connectTCP(self, host, port, timeout=30, bindAddress=None):
        factory = AmqpFactory(*self.args, **self.kwargs)
        reactor.connectTCP(host, port, factory, timeout=timeout, bindAddress=bindAddress)

