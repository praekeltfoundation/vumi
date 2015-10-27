# -*- test-case-name: vumi.tests.test_fake_amqp -*-

from uuid import uuid4
import re

from pika.frame import decode_frame, Method, Header, Body
from pika.spec import (
    Connection, Channel, Exchange, Queue, Basic, BasicProperties)
from twisted.internet import reactor
from twisted.internet.defer import Deferred, succeed
from twisted.protocols.loopback import loopbackAsync
from twisted.internet.protocol import Protocol

from vumi.service import WorkerAMQClient
from vumi.message import Message as VumiMessage
from vumi.tests.fake_connection import wait0


def gen_id(prefix=''):
    return ''.join([prefix, uuid4().get_hex()])


def gen_longlong():
    return uuid4().int & 0xffffffffffffffff


class FakeAMQPBroker(object):
    def __init__(self):
        self.queues = {}
        self.exchanges = {}
        self.server_protocols = []
        self.dispatched = {}
        self._content_pending = None

    def _get_queue(self, queue):
        assert queue in self.queues
        return self.queues[queue]

    def _get_exchange(self, exchange):
        assert exchange in self.exchanges
        return self.exchanges[exchange]

    def exchange_declare(self, exchange, exchange_type):
        exchange_class = None
        if exchange_type == 'direct':
            exchange_class = FakeAMQPExchangeDirect
        elif exchange_type == 'topic':
            exchange_class = FakeAMQPExchangeTopic
        assert exchange_class is not None
        self.exchanges.setdefault(exchange, exchange_class(exchange))
        assert exchange_type == self.exchanges[exchange].exchange_type
        return Exchange.DeclareOk()

    def queue_declare(self, queue):
        if not queue:
            queue = gen_id('queue.')
        self.queues.setdefault(queue, FakeAMQPQueue(queue))
        queue_obj = self._get_queue(queue)
        return Queue.DeclareOk(
            queue=queue,
            message_count=queue_obj.message_count(),
            consumer_count=queue_obj.consumer_count())

    def queue_bind(self, queue, exchange, routing_key):
        self._get_exchange(exchange).queue_bind(
            routing_key, self._get_queue(queue))
        return Queue.BindOk()

    def basic_consume(self, queue, tag):
        self._get_queue(queue).add_consumer(tag)
        self.kick_delivery()

    def basic_cancel(self, tag, queue):
        if queue in self.queues:
            self.queues[queue].remove_consumer(tag)

    def basic_publish(self, exchange, routing_key, content):
        exc = self.dispatched.setdefault(exchange, {})
        exc.setdefault(routing_key, []).append(content)
        if exchange not in self.exchanges:
            # This is to test, so we don't care about missing queues
            return None
        self._get_exchange(exchange).basic_publish(routing_key, content)
        self.kick_delivery()
        return None

    def basic_get(self, queue):
        return self._get_queue(queue).get_message()

    def basic_ack(self, queue, delivery_tag):
        self._get_queue(queue).ack(delivery_tag)
        return None

    def deliver_to_channels(self):
        # Since all delivery goes through kick_delivery(), this can
        # only happen if message_processed() is called too many times.
        assert self._content_pending is not None

        for server_protocol in self.server_protocols:
            for channel in server_protocol.channels.values():
                self.try_deliver_to_channel(channel)

        # Process the sentinel "message" we added in kick_delivery().
        self.message_processed()

    def try_deliver_to_channel(self, channel):
        delivered = False
        for ctag, queue in channel.consumers.items():
            while channel.deliverable(ctag):
                dtag, msg = self._get_queue(queue).get_message()
                if dtag is None:
                    break
                dmsg = Basic.Deliver(
                    ctag, dtag, False, msg['exchange'], msg['routing_key'])
                self._content_pending['delivering_count'] += 1
                channel.deliver_message(dmsg, msg['content'])
                delivered = True
        return delivered

    def set_content_pending(self):
        if self._content_pending is None:
            self._content_pending = {
                'deferred': Deferred(),
                'delivering_count': 0,
                'publishing_count': 0,
            }

    def kick_delivery(self):
        """
        Schedule a message delivery run.

        Returns a deferred that will fire when all deliverable
        messages have been delivered and processed by their consumers.
        This is useful for manually triggering a delivery run from
        inside a test.
        """
        self.set_content_pending()
        # Add a sentinel "message" that gets processed after this
        # delivery run, making the delivery process re-entrant. This
        # is important, because delivered messages can trigger more
        # messages to be published, which kicks delivery again.
        self._content_pending['delivering_count'] += 1
        # Schedule this for later, so that we don't block whatever it
        # is we're currently doing.
        reactor.callLater(0, self.deliver_to_channels)
        return self.wait_delivery()

    def wait_delivery(self):
        """
        Wait for the current message delivery run (if any) to finish.

        Returns a deferred that will fire when the broker is finished
        delivering any messages from the current run. This should not
        leave any messages undelivered, because basic_publish() kicks
        off a delivery run and we wait for the reactor to start it if
        it's still scheduled for the next tick.

        NOTE: This method should be called during test teardown to make
        sure there are no pending delivery cleanups that will cause a
        dirty reactor race.
        """
        return wait0().addCallback(self._wait_delivery_cb)

    def _wait_delivery_cb(self, r):
        if self._content_pending is not None:
            return self._content_pending['deferred']
        return succeed(None)

    def wait_messages(self, exchange, rkey, n):
        def check(d):
            msgs = self.get_messages(exchange, rkey)
            if len(msgs) >= n:
                d.callback(msgs)
            else:
                reactor.callLater(0, check, d)

        done = Deferred()
        reactor.callLater(0, check, done)
        return done

    def clear_messages(self, exchange, rkey=None):
        if exchange not in self.dispatched:
            return
        if rkey:
            del self.dispatched[exchange][rkey][:]
        else:
            self.dispatched[exchange].clear()

    def get_dispatched(self, exchange, rkey):
        return self.dispatched.get(exchange, {}).get(rkey, [])

    def get_messages(self, exchange, rkey):
        contents = self.get_dispatched(exchange, rkey)
        messages = [VumiMessage.from_json(content) for content in contents]
        return messages

    def publish_message(self, exchange, routing_key, message):
        return self.publish_raw(exchange, routing_key, message.to_json())

    def publish_raw(self, exchange, routing_key, data):
        assert exchange in self.exchanges
        return self.basic_publish(exchange, routing_key, data)

    def check_pending_content(self):
        if self._content_pending['delivering_count'] > 0:
            return
        if self._content_pending['publishing_count'] > 0:
            return
        d = self._content_pending['deferred']
        self._content_pending = None
        d.callback(None)

    def start_publishing(self):
        self.set_content_pending()
        self._content_pending['publishing_count'] += 1

    def finish_publishing(self):
        assert self._content_pending is not None
        self._content_pending['publishing_count'] -= 1
        self.check_pending_content()

    def message_processed(self):
        assert self._content_pending is not None
        self._content_pending['delivering_count'] -= 1
        self.check_pending_content()


class FakeAMQPChannel(object):
    def __init__(self, channel_id, server_protocol):
        self.channel_id = channel_id
        self.server_protocol = server_protocol
        self.broker = server_protocol.broker
        self.qos_prefetch_count = 0
        self.consumers = {}
        self.unacked = []
        self._consumer_prefetch = {}
        self._publishing = None

    def __repr__(self):
        return '<FakeAMQPChannel: id=%s>' % (self.channel_id,)

    def channel_open(self):
        assert self.channel_id not in self.server_protocol.channels
        self.server_protocol.channels[self.channel_id] = self
        return Channel.OpenOk()

    def channel_close(self):
        self.server_protocol.channels.pop(self.channel_id)
        return Channel.CloseOk()

    def channel_flow(self, active):
        raise NotImplementedError(
            "channel.flow() is no longer supported in RabbitMQ 3.3.0.")

    def close(self, _reason):
        pass

    def basic_qos(self, prefetch_size, prefetch_count, is_global):
        if is_global:
            raise NotImplementedError("global prefetch limits not supported.")
        self.qos_prefetch_count = prefetch_count
        return Basic.QosOk()

    def exchange_declare(self, exchange, type, durable=None):
        return self.broker.exchange_declare(exchange, type)

    def queue_declare(self, queue, durable=None):
        return self.broker.queue_declare(queue)

    def queue_bind(self, queue, exchange, routing_key):
        return self.broker.queue_bind(queue, exchange, routing_key)

    def basic_consume(self, queue, tag=None):
        if not tag:
            tag = gen_id('consumer.')
        assert tag not in self.consumers
        self._consumer_prefetch[tag] = self.qos_prefetch_count
        self.consumers[tag] = queue
        self.broker.basic_consume(queue, tag)
        return Basic.ConsumeOk(consumer_tag=tag)

    def basic_cancel(self, tag):
        queue = self.consumers.pop(tag, None)
        if queue:
            self.broker.basic_cancel(tag, queue)
        self._consumer_prefetch.pop(tag, None)
        return Basic.CancelOk(consumer_tag=tag)

    def basic_publish(self, exchange, routing_key):
        self._publishing = {
            'exchange': exchange,
            'routing_key': routing_key,
        }
        self.broker.start_publishing()

    def basic_ack(self, delivery_tag, multiple):
        assert delivery_tag in [dtag for dtag, _ctag, _queue in self.unacked]
        for dtag, ctag, queue in self.unacked[:]:
            if multiple or (dtag == delivery_tag):
                self.unacked.remove((dtag, ctag, queue))
                if ctag is not None and ctag not in self.consumers:
                    raise Exception("Invalid consumer tag: %s" % (ctag,))
                resp = self.broker.basic_ack(queue, dtag)
                if (dtag == delivery_tag):
                    return resp

    def _get_consumer_prefetch(self, consumer_tag):
        return self._consumer_prefetch[consumer_tag]

    def deliverable(self, consumer_tag):
        if consumer_tag not in self.consumers:
            return False
        prefetch = self._get_consumer_prefetch(consumer_tag)
        if prefetch < 1:
            return True
        return len(self.unacked) < prefetch

    def deliver_message(self, method, body, unack=True):
        if unack:
            ctag = method.consumer_tag
            self.unacked.append(
                (method.delivery_tag, ctag, self.consumers[ctag]))
        self.server_protocol.send_method(self.channel_id, method)
        self._send_body(body)

    def basic_get(self, queue):
        dtag, msg = self.broker.basic_get(queue)
        if msg:
            self.unacked.append((dtag, None, queue))
            method = Basic.GetOk(
                dtag, False, msg['exchange'], msg['routing_key'], 1)
            return self.deliver_message(method, msg['content'], unack=False)
        return Basic.GetEmpty()

    def _send_body(self, body):
        self.server_protocol.send_frame(Header(
            self.channel_id, len(body), BasicProperties()))
        self.server_protocol.send_frame(Body(self.channel_id, body))

    def message_processed(self):
        """
        Notify the broker that a message has been processed, in order
        to make delivery sane.
        """
        self.broker.message_processed()

    def _reset_publishing(self):
        if self._publishing is not None:
            self._publishing = None
            self.broker.finish_publishing()

    def _dispatch_method(self, method):
        self._reset_publishing()
        if method.NAME == Channel.Open.NAME:
            return self.channel_open()
        elif method.NAME == Exchange.Declare.NAME:
            return self.exchange_declare(method.exchange, method.type)
        elif method.NAME == Queue.Declare.NAME:
            return self.queue_declare(method.queue)
        elif method.NAME == Queue.Bind.NAME:
            return self.queue_bind(
                method.queue, method.exchange, method.routing_key)
        elif method.NAME == Basic.Consume.NAME:
            return self.basic_consume(method.queue, method.consumer_tag)
        elif method.NAME == Basic.Publish.NAME:
            return self.basic_publish(method.exchange, method.routing_key)
        elif method.NAME == Basic.Ack.NAME:
            return self.basic_ack(method.delivery_tag, method.multiple)
        elif method.NAME == Basic.Cancel.NAME:
            return self.basic_cancel(method.consumer_tag)
        elif method.NAME == Basic.Get.NAME:
            return self.basic_get(method.queue)
        elif method.NAME == Basic.Qos.NAME:
            return self.basic_qos(
                method.prefetch_size, method.prefetch_count, method.global_)

        assert False, "Unexpected method: %s" % method.NAME

    def _handle_header(self, header):
        assert self._publishing is not None, "Header without publishing."
        assert 'remaining' not in self._publishing, "Already seen header."
        self._publishing['remaining'] = header.body_size
        self._publishing['body'] = ""
        self._check_publish_done()

    def _check_publish_done(self):
        if self._publishing['remaining'] > 0:
            return
        self.broker.basic_publish(
            self._publishing['exchange'],
            self._publishing['routing_key'],
            self._publishing['body'])
        self._reset_publishing()

    def _handle_body(self, body):
        assert self._publishing is not None, "Body without publishing."
        assert 'remaining' in self._publishing, "Body without header."
        self._publishing['body'] += body.fragment
        self._publishing['remaining'] -= len(body.fragment)
        self._check_publish_done()


class FakeAMQPExchange(object):
    def __init__(self, name):
        self.name = name
        self.binds = {}

    def queue_bind(self, routing_key, queue):
        binds = self.binds.setdefault(routing_key, set())
        binds.add(queue)

    def basic_publish(self, routing_key, content):
        raise NotImplementedError()


class FakeAMQPExchangeDirect(FakeAMQPExchange):
    exchange_type = 'direct'

    def basic_publish(self, routing_key, content):
        for queue in self.binds.get(routing_key, set()):
            queue.put(self.name, routing_key, content)


class FakeAMQPExchangeTopic(FakeAMQPExchange):
    exchange_type = 'topic'

    def _bind_regex(self, bind):
        for k, v in [('.', r'\.'),
                     ('*', r'[^.]+'),
                     ('\.#\.', r'\.([^.]+\.)*'),
                     ('#\.', r'([^.]+\.)*'),
                     ('\.#', r'(\.[^.]+)*')]:
            bind = '^%s$' % bind.replace(k, v)
        return re.compile(bind)

    def match_rkey(self, bind, rkey):
        return (self._bind_regex(bind).match(rkey) is not None)

    def basic_publish(self, routing_key, content):
        for bind, queues in self.binds.items():
            if self.match_rkey(bind, routing_key):
                for queue in queues:
                    queue.put(self.name, routing_key, content)


class FakeAMQPQueue(object):
    def __init__(self, name):
        self.name = name
        self.messages = []
        self.consumers = set()
        self.unacked_messages = {}

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def add_consumer(self, consumer_tag):
        if consumer_tag not in self.consumers:
            self.consumers.add(consumer_tag)

    def remove_consumer(self, consumer_tag):
        if consumer_tag in self.consumers:
            self.consumers.remove(consumer_tag)

    def message_count(self):
        return len(self.messages)

    def consumer_count(self):
        return len(self.consumers)

    def put(self, exchange, routing_key, content):
        self.messages.append({
            'exchange': exchange,
            'routing_key': routing_key,
            'content': content,
        })

    def ack(self, delivery_tag):
        self.unacked_messages.pop(delivery_tag)

    def get_message(self):
        try:
            msg = self.messages.pop(0)
        except IndexError:
            return (None, None)
        dtag = gen_longlong()
        self.unacked_messages[dtag] = msg
        return (dtag, msg)


class FakeAMQPServerProtocol(Protocol):
    """
    A very basic wrapper around pika's AMQP wire protocol implementation.
    """

    def __init__(self, broker):
        self._buffer = b""
        self.broker = broker
        self.broker.server_protocols.append(self)
        self.conn_state = "NEW"
        self.channels = {}

    def dataReceived(self, data):
        self._buffer = self._parse_frames(self._buffer + data)

    def _parse_frames(self, data):
        bytes_consumed, frame = decode_frame(data)
        while bytes_consumed > 0:
            try:
                self._handle_frame(frame)
            except:
                # These exceptions would otherwise vanish beyond our reach.
                import traceback
                traceback.print_exc()
                raise
            data = data[bytes_consumed:]
            bytes_consumed, frame = decode_frame(data)
        return data

    def _handle_frame(self, frame):
        if self.conn_state == "NEW":
            return self._handle_connection_header(frame)
        elif self.conn_state == "CONNECTING":
            return self._handle_connecting_method(frame.method)
        assert self.conn_state == "CONNECTED"
        if frame.NAME == Method.NAME:
            self._handle_method(frame.channel_number, frame.method)
        elif frame.NAME == "Header":
            self.channels[frame.channel_number]._handle_header(frame)
        elif frame.NAME == "Body":
            self.channels[frame.channel_number]._handle_body(frame)

    def _handle_connection_header(self, header):
        assert header.NAME == "ProtocolHeader"
        self.conn_state = "CONNECTING"
        self.send_method(0, Connection.Start())

    def _handle_connecting_method(self, method):
        if method.NAME == Connection.StartOk.NAME:
            return self.send_method(0, Connection.Tune(
                channel_max=1024, frame_max=1024*1024))
        elif method.NAME == Connection.TuneOk.NAME:
            return
        elif method.NAME == Connection.Open.NAME:
            self.conn_state = "CONNECTED"
            return self.send_method(0, Connection.OpenOk())
        assert False, "Unexpected method while connecting: %s" % method.NAME

    def _handle_method(self, channel_number, method):
        if channel_number == 0:
            return self.send_method(0, self._handle_connection_method(method))
        if method.NAME == Channel.Open.NAME:
            channel = FakeAMQPChannel(channel_number, self)
        else:
            channel = self.channels[channel_number]
        resp = channel._dispatch_method(method)
        if resp is not None:
            return self.send_method(channel_number, resp)

    def connectionLost(self, reason):
        self.connected = False

    def send_method(self, channel, method):
        """
        Write an AMQP method frame.
        """
        return self.send_frame(Method(channel, method))

    def send_frame(self, frame):
        return self.write(frame.marshal())

    def write(self, data):
        """
        Write some bytes and allow the reactor to send them.
        """
        self.transport.write(data)
        return wait0()


class FakeAMQClient(WorkerAMQClient):
    def __init__(self, spec, vumi_options=None, broker=None):
        from txamqp.client import TwistedDelegate
        WorkerAMQClient.__init__(self, TwistedDelegate(), '', spec)
        if vumi_options is not None:
            self.vumi_options = vumi_options
        self.broker = broker

    def connected_callback(self, client):
        pass

    def _patch_channel(self, channel):
        channel.message_processed = self.broker.message_processed
        return channel

    def channel(self, id):
        d = WorkerAMQClient.channel(self, id)
        return d.addCallback(self._patch_channel)


def make_fake_client(spec, vumi_options=None, broker=None):
    if broker is None:
        broker = FakeAMQPBroker()
    server = FakeAMQPServerProtocol(broker)
    client = FakeAMQClient(spec, vumi_options, broker)
    client._server = server
    client._finished_d = loopbackAsync(server, client)
    return client
