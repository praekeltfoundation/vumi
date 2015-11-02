import pika
from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.internet.defer import (
    Deferred, succeed, inlineCallbacks, returnValue)
from twisted.internet.protocol import ClientFactory

from vumi.reconnecting_client import ReconnectingClientService


def _fire_and_return(r, d):
    d.callback(r)
    return r


def sub_deferred(parent_d):
    d = Deferred()
    parent_d.addCallback(_fire_and_return, d)
    return d


class AMQPClientService(ReconnectingClientService):
    """
    A service that manages an AMQP client connection
    """
    _client = None

    def __init__(self, endpoint):
        factory = PikaClientFactory(pika.ConnectionParameters())
        ReconnectingClientService.__init__(self, endpoint, factory)
        self._connect_d = Deferred()

    def await_connected(self):
        return sub_deferred(self._connect_d)

    def clientConnected(self, protocol):
        ReconnectingClientService.clientConnected(self, protocol)
        self._client = protocol
        # If the client has a _fake_server attribute, we want to grab that so
        # test code doesn't need to dig around in our guts.
        if hasattr(protocol, '_fake_server'):
            self._fake_server = protocol._fake_server
        # `protocol.ready` is a Deferred that fires when the AMQP connection is
        # open and is set to `None` after that. We need to handle both cases
        # because the network might be faster than us.
        d = succeed(None).addCallback(lambda _: protocol.ready)
        return d.addCallback(self.ready_callback)

    def clientConnectionLost(self, reason):
        self._client = None
        self._connect_d = Deferred()
        ReconnectingClientService.clientConnectionLost(self, reason)

    def ready_callback(self, _ignored):
        self._connect_d.callback(self)

    def get_client(self):
        # TODO: Better exception.
        assert self._client is not None, "AMQP not connected."
        return self._client

    def _declare_exchange(self, source, channel):
        # get the details for AMQP
        exchange_name = source.exchange_name
        exchange_type = source.exchange_type
        durable = source.durable
        return channel.exchange_declare(exchange=exchange_name,
                                        type=exchange_type, durable=durable)

    @inlineCallbacks
    def get_channel(self):
        channel = yield self.get_client().channel()
        patch_channel(channel, getattr(self, '_fake_server', None))
        returnValue(channel)

    @inlineCallbacks
    def start_consumer(self, consumer_class, *args, **kwargs):
        channel = yield self.get_channel()

        consumer = consumer_class(channel, *args, **kwargs)
        # consumer.vumi_options = self.vumi_options

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
        channel = yield self.get_channel()
        # start the publisher
        publisher = publisher_class(*args, **kwargs)
        # publisher.vumi_options = self.vumi_options
        # declare the exchange, doesn't matter if it already exists
        yield self._declare_exchange(publisher, channel)
        # start!
        yield publisher.start(channel)
        # return the publisher
        returnValue(publisher)


def patch_channel(channel, fake_server):
    """
    Patch a channel object to fix some cases where it might wait forever.
    """
    orig_basic_cancel = channel.basic_cancel

    def basic_cancel(consumer_tag):
        if consumer_tag not in channel.consumer_tags:
            return succeed(None)
        return orig_basic_cancel(consumer_tag=consumer_tag)
    channel.basic_cancel = basic_cancel

    def on_getempty(frame):
        channel._on_getok_callback(None, frame.method, None, None)
    channel.add_callback(on_getempty, ['Basic.GetEmpty'])

    if fake_server is not None:
        patch_fake_channel(channel, fake_server)


def patch_fake_channel(channel, fake_server):
    """
    Patch a channel object connected to a fake broker with some things that are
    useful in tests but don't exist in production.
    """
    channel.message_processed = fake_server.broker.message_processed
    channel._fake_channel = fake_server.channels[channel.channel_number]
    orig_basic_publish = channel.basic_publish

    def basic_publish(*args, **kw):
        return orig_basic_publish(*args, **kw).addCallback(
            fake_server.broker.wait0)
    channel.basic_publish = basic_publish


class PikaClientFactory(ClientFactory):
    def __init__(self, connection_parameters):
        self.connection_parameters = connection_parameters

    def protocol(self):
        return TwistedProtocolConnection(self.connection_parameters)
