# -*- test-case-name: vumi.tests.test_fake_amqp -*-

from uuid import uuid4
import re

from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from txamqp.client import TwistedDelegate
from txamqp.content import Content

from vumi.service import WorkerAMQClient
from vumi.message import Message as VumiMessage


def gen_id(prefix=''):
    return ''.join([prefix, uuid4().get_hex()])


def gen_longlong():
    return uuid4().int & 0xffffffffffffffff


class Thing(object):
    """
    A generic thing to reply with.
    """
    def __init__(self, kind, **kw):
        self._kind = kind
        self._kwfields = kw.keys()
        for k, v in kw.items():
            setattr(self, k, v)

    def __str__(self):
        return "<Thing:: %s %s>" % (self._kind,
                                    ['[%s: %s]' % (f, getattr(self, f))
                                     for f in self._kwfields])


class Message(object):
    """
    A message is more complicated than a Thing.
    """
    def __init__(self, method, fields=(), content=None):
        self.method = method
        self._fields = fields
        self.content = content

    def __getattr__(self, key):
        for k, v in self._fields:
            if k == key:
                return v
        raise AttributeError(key)


def mkMethod(name, index=-1):
    """
    Create a "Method" object, suitable for a ``txamqp`` message.

    :param name: The name of the AMQP method, per the XML spec.
    :param index: The index of the AMQP method, per the XML spec.
    """
    return Thing("Method", name=name, id=index)


def mkContent(body, children=None, properties=None):
    return Thing("Content", body=body, children=children,
                 properties=properties)


def mk_deliver(body, exchange, routing_key, ctag, dtag):
    return Message(mkMethod('deliver', 60), [
            ('consumer_tag', ctag),
            ('delivery_tag', dtag),
            ('redelivered', False),
            ('exchange', exchange),
            ('routing_key', routing_key),
            ], mkContent(body))


def mk_get_ok(body, exchange, routing_key, dtag):
    return Message(mkMethod('get-ok', 71), [
            ('delivery_tag', dtag),
            ('redelivered', False),
            ('exchange', exchange),
            ('routing_key', routing_key),
            ], mkContent(body))


class FakeAMQPBroker(object):
    def __init__(self):
        self.queues = {}
        self.exchanges = {}
        self.channels = []
        self.dispatched = {}
        self._delivering = None

    def _get_queue(self, queue):
        assert queue in self.queues
        return self.queues[queue]

    def _get_exchange(self, exchange):
        assert exchange in self.exchanges
        return self.exchanges[exchange]

    def channel_open(self, channel):
        assert channel not in self.channels
        self.channels.append(channel)
        return Message(mkMethod("open-ok", 11))

    def channel_close(self, channel):
        if channel in self.channels:
            self.channels.remove(channel)
        return Message(mkMethod("close-ok", 41))

    def exchange_declare(self, exchange, exchange_type):
        exchange_class = None
        if exchange_type == 'direct':
            exchange_class = FakeAMQPExchangeDirect
        elif exchange_type == 'topic':
            exchange_class = FakeAMQPExchangeTopic
        assert exchange_class is not None
        self.exchanges.setdefault(exchange, exchange_class(exchange))
        assert exchange_type == self.exchanges[exchange].exchange_type
        return Message(mkMethod("declare-ok", 11))

    def queue_declare(self, queue):
        if not queue:
            queue = gen_id('queue.')
        self.queues.setdefault(queue, FakeAMQPQueue(queue))
        queue_obj = self._get_queue(queue)
        return Message(mkMethod("declare-ok", 11), [
                ('queue', queue),
                ('message_count', queue_obj.message_count()),
                ('consumer_count', queue_obj.consumer_count()),
                ])

    def queue_bind(self, queue, exchange, routing_key):
        self._get_exchange(exchange).queue_bind(routing_key,
                                            self._get_queue(queue))
        return Message(mkMethod("bind-ok", 21))

    def basic_consume(self, queue, tag):
        self._get_queue(queue).add_consumer(tag)
        self.kick_delivery()
        return Message(mkMethod("consume-ok", 21), [("consumer_tag", tag)])

    def basic_cancel(self, tag, queue):
        if queue in self.queues:
            self.queues[queue].remove_consumer(tag)
        return Message(mkMethod("cancel-ok", 31), [("consumer_tag", tag)])

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
        assert self._delivering is not None

        for channel in self.channels:
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
                dmsg = mk_deliver(msg['content'], msg['exchange'],
                                  msg['routing_key'], ctag, dtag)
                self._delivering['count'] += 1
                channel.deliver_message(dmsg, ctag)
                delivered = True
        return delivered

    def kick_delivery(self):
        """
        Schedule a message delivery run.

        Returns a deferred that will fire when all deliverable
        messages have been delivered and processed by their consumers.
        This is useful for manually triggering a delivery run from
        inside a test.
        """
        if self._delivering is None:
            self._delivering = {
                'deferred': Deferred(),
                'count': 0,
                }
        # Add a sentinel "message" that gets processed after this
        # delivery run, making the delivery process re-entrant. This
        # is important, because delivered messages can trigger more
        # messages to be published, which kicks delivery again.
        self._delivering['count'] += 1
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
        off a delivery run.

        NOTE: This method should be called during test teardown to make
        sure there are no pending delivery cleanups that will cause a
        dirty reactor race.
        """
        if self._delivering is not None:
            return self._delivering['deferred']
        d = Deferred()
        d.callback(None)
        return d

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
        messages = [VumiMessage.from_json(content.body)
                    for content in contents]
        return messages

    def publish_message(self, exchange, routing_key, message):
        return self.publish_raw(exchange, routing_key, message.to_json())

    def publish_raw(self, exchange, routing_key, data):
        assert exchange in self.exchanges
        amq_message = Content(data)
        return self.basic_publish(exchange, routing_key, amq_message)

    def message_processed(self):
        assert self._delivering is not None
        self._delivering['count'] -= 1
        if self._delivering['count'] <= 0:
            d = self._delivering['deferred']
            self._delivering = None
            d.callback(None)


class FakeAMQPChannel(object):
    def __init__(self, channel_id, client):
        self.channel_id = channel_id
        self.client = client
        self.broker = client.broker
        self.qos_prefetch_count = 0
        self.consumers = {}
        self.delegate = client.delegate
        self.unacked = []
        self._consumer_prefetch = {}

    def __repr__(self):
        return '<FakeAMQPChannel: id=%s>' % (self.channel_id,)

    def channel_open(self):
        return self.broker.channel_open(self)

    def channel_close(self):
        return self.broker.channel_close(self)

    def channel_flow(self, active):
        raise NotImplementedError(
            "channel.flow() is no longer supported in RabbitMQ 3.3.0.")

    def close(self, _reason):
        pass

    def basic_qos(self, _prefetch_size, prefetch_count, is_global):
        if is_global:
            raise NotImplementedError("global prefetch limits not supported.")
        self.qos_prefetch_count = prefetch_count

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
        return self.broker.basic_consume(queue, tag)

    def basic_cancel(self, tag):
        queue = self.consumers.pop(tag, None)
        if queue:
            self.broker.basic_cancel(tag, queue)
        self._consumer_prefetch.pop(tag, None)
        return Message(mkMethod("cancel-ok", 31))

    def basic_publish(self, exchange, routing_key, content):
        return self.broker.basic_publish(exchange, routing_key, content)

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

    def deliver_message(self, msg, consumer_tag):
        self.unacked.append(
            (msg.delivery_tag, consumer_tag, self.consumers[consumer_tag]))
        self.delegate.basic_deliver(self, msg)

    def basic_get(self, queue):
        dtag, msg = self.broker.basic_get(queue)
        if msg:
            self.unacked.append((dtag, None, queue))
            return mk_get_ok(msg['content'], msg['exchange'],
                             msg['routing_key'], dtag)
        return Message(mkMethod("get-empty", 72))

    def message_processed(self):
        """
        Notify the broker that a message has been processed, in order
        to make delivery sane.
        """
        self.broker.message_processed()


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
                'content': content.body,
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


class FakeAMQClient(WorkerAMQClient):
    def __init__(self, spec, vumi_options=None, broker=None):
        WorkerAMQClient.__init__(self, TwistedDelegate(), '', spec)
        if vumi_options is not None:
            self.vumi_options = vumi_options
        if broker is None:
            broker = FakeAMQPBroker()
        self.broker = broker

    @inlineCallbacks
    def channel(self, id):
        yield self.channelLock.acquire()
        try:
            try:
                ch = self.channels[id]
            except KeyError:
                ch = FakeAMQPChannel(id, self)
                self.channels[id] = ch
        finally:
            self.channelLock.release()
        returnValue(ch)
