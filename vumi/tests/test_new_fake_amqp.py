from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.service import get_spec, Worker
from vumi.utils import vumi_resource_path
from vumi.tests import new_fake_amqp
from vumi.tests.helpers import VumiTestCase


class ToyWorker(Worker):
    @inlineCallbacks
    def startWorker(self):
        paused = self.config.get('paused', False)
        self.msgs = []
        self.pub = yield self.publish_to('test.pub')
        self.conpub = yield self.publish_to('test.con')
        self.con = yield self.consume(
            'test.con', self.consume_msg, paused=paused)

    def consume_msg(self, msg):
        self.msgs.append(msg)


class TestFakeAMQP(VumiTestCase):
    def setUp(self):
        self.broker = new_fake_amqp.FakeAMQPBroker()
        self.add_cleanup(self.broker.wait_delivery)

    def make_client(self):
        spec = get_spec(vumi_resource_path("amqp-spec-0-9-1.xml"))
        amq_client = new_fake_amqp.make_fake_client(spec, {
            "username": "guest",
            "password": "guest",
        }, self.broker)
        return amq_client.started.wait().addCallback(lambda _: amq_client)

    @inlineCallbacks
    def make_channel(self, client=None):
        if client is None:
            client = yield self.make_client()
        channel = yield client.get_channel()
        returnValue((channel, client._server.channels[channel.id]))

    @inlineCallbacks
    def get_worker(self, **config):
        worker = ToyWorker({}, config)
        worker._amqp_client = yield self.make_client()
        yield worker.startWorker()
        returnValue(worker)

    @inlineCallbacks
    def test_channel_open(self):
        """
        channel_open() creates a new channel.
        """
        client = yield self.make_client()
        self.assertEqual(client._server.channels, {})
        channel = yield client.get_channel()
        self.assertEqual(channel.id, 1)
        self.assertEqual(set(client._server.channels.keys()), set([1]))
        channel = yield client.get_channel()
        self.assertEqual(channel.id, 2)
        self.assertEqual(set(client._server.channels.keys()), set([1, 2]))

    @inlineCallbacks
    def test_exchange_declare(self):
        """
        exchange_declare() creates a new exchange.
        """
        channel, server_channel = yield self.make_channel()
        self.assertEqual({}, self.broker.exchanges)

        yield channel.exchange_declare(exchange='foo', type='direct')
        self.assertEqual(['foo'], self.broker.exchanges.keys())
        self.assertEqual('direct', self.broker.exchanges['foo'].exchange_type)

        yield channel.exchange_declare(exchange='bar', type='topic')
        self.assertEqual(['bar', 'foo'], sorted(self.broker.exchanges.keys()))
        self.assertEqual('topic', self.broker.exchanges['bar'].exchange_type)

    @inlineCallbacks
    def test_queue_declare_and_bind(self):
        """
        queue_declare() creates a new queue and queue_bind() binds it to a
        routing key.
        """
        channel, server_channel = yield self.make_channel()
        self.assertEqual({}, self.broker.queues)

        yield channel.queue_declare(queue='foo')
        yield channel.queue_declare(queue='foo')
        self.assertEqual(['foo'], self.broker.queues.keys())

        yield channel.exchange_declare(exchange='exch', type='direct')
        exch = self.broker.exchanges['exch']
        self.assertEqual({}, exch.binds)

        yield channel.queue_bind(
            queue='foo', exchange='exch', routing_key='routing.key')
        self.assertEqual(['routing.key'], exch.binds.keys())

        n = len(self.broker.queues)
        yield channel.queue_declare(queue='')
        self.assertEqual(n + 1, len(self.broker.queues))

    @inlineCallbacks
    def test_publish_direct(self):
        """
        Direct exchanges deliver to the right queues.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='ed', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_declare(queue='q2')
        yield channel.queue_declare(queue='q3')

        yield channel.queue_bind(
            queue='q1', exchange='ed', routing_key='routing.key.one')
        yield channel.queue_bind(
            queue='q1', exchange='ed', routing_key='routing.key.two')
        yield channel.queue_bind(
            queue='q2', exchange='ed', routing_key='routing.key.two')

        delivered = []

        def mfp(q):
            return lambda *args: delivered.append((q,) + args)

        self.broker.queues['q1'].put = mfp('q1')
        self.broker.queues['q2'].put = mfp('q2')
        self.broker.queues['q3'].put = mfp('q3')

        self.broker.basic_publish('ed', 'routing.key.none', 'blah')
        self.assertEqual([], delivered)

        self.broker.basic_publish('ed', 'routing.key.*', 'blah')
        self.assertEqual([], delivered)

        self.broker.basic_publish('ed', 'routing.key.#', 'blah')
        self.assertEqual([], delivered)

        self.broker.basic_publish('ed', 'routing.key.one', 'blah')
        self.assertEqual([('q1', 'ed', 'routing.key.one', 'blah')], delivered)

        delivered[:] = []  # Clear without reassigning
        self.broker.basic_publish('ed', 'routing.key.two', 'blah')
        self.assertEqual([('q1', 'ed', 'routing.key.two', 'blah'),
                          ('q2', 'ed', 'routing.key.two', 'blah')],
                         sorted(delivered))

    @inlineCallbacks
    def test_publish_topic(self):
        """
        Topic exchanges deliver to the right queues.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='et', type='topic')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_declare(queue='q2')
        yield channel.queue_declare(queue='q3')

        yield channel.queue_bind(
            queue='q1', exchange='et', routing_key='routing.key.*.foo.#')
        yield channel.queue_bind(
            queue='q2', exchange='et', routing_key='routing.key.#.foo')
        yield channel.queue_bind(
            queue='q3', exchange='et', routing_key='routing.key.*.foo.*')

        delivered = []

        def mfp(q):
            return lambda *args: delivered.append((q,) + args)

        self.broker.queues['q1'].put = mfp('q1')
        self.broker.queues['q2'].put = mfp('q2')
        self.broker.queues['q3'].put = mfp('q3')

        self.broker.basic_publish('et', 'routing.key.none', 'blah')
        self.assertEqual([], delivered)

        self.broker.basic_publish('et', 'routing.key.foo.one', 'blah')
        self.assertEqual([], delivered)

        self.broker.basic_publish('et', 'routing.key.foo', 'blah')
        self.assertEqual([('q2', 'et', 'routing.key.foo', 'blah')],
                         delivered)

        delivered[:] = []  # Clear without reassigning
        self.broker.basic_publish('et', 'routing.key.one.two.foo', 'blah')
        self.assertEqual([('q2', 'et', 'routing.key.one.two.foo', 'blah')],
                         delivered)

        delivered[:] = []  # Clear without reassigning
        self.broker.basic_publish('et', 'routing.key.one.foo', 'blah')
        self.assertEqual([('q1', 'et', 'routing.key.one.foo', 'blah'),
                          ('q2', 'et', 'routing.key.one.foo', 'blah'),
                          ], sorted(delivered))

        delivered[:] = []  # Clear without reassigning
        self.broker.basic_publish('et', 'routing.key.one.foo.two', 'blah')
        self.assertEqual([('q1', 'et', 'routing.key.one.foo.two', 'blah'),
                          ('q3', 'et', 'routing.key.one.foo.two', 'blah'),
                          ], sorted(delivered))

    @inlineCallbacks
    def test_basic_get(self):
        """
        basic_get() will retrieve a single waiting message.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='e1', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_bind(queue='q1', exchange='e1', routing_key='rkey')
        self.assertEqual(len(server_channel.unacked), 0)

        yield self.broker.basic_publish('e1', 'rkey', 'blah')
        reply = yield channel.basic_get(queue='q1')
        self.assertEqual(reply.method.name, 'get-ok')
        self.assertEqual(reply.content.body, 'blah')
        self.assertEqual(len(server_channel.unacked), 1)

    @inlineCallbacks
    def test_basic_get_nothing(self):
        """
        basic_get() will return get-empty if there is no message to get.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='e1', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_bind(queue='q1', exchange='e1', routing_key='rkey')
        self.assertEqual(len(server_channel.unacked), 0)

        reply = yield channel.basic_get(queue='q1')
        self.assertEqual(reply.method.name, 'get-empty')

    @inlineCallbacks
    def test_consumer_wrangling(self):
        """
        Various things can be done with consumers.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='e1', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_bind(queue='q1', exchange='e1', routing_key='foo')
        q1 = self.broker.queues['q1']
        self.assertEqual(set(), q1.consumers)

        yield channel.basic_consume(queue='q1', consumer_tag='tag1')
        self.assertEqual(set(['tag1']), q1.consumers)
        yield channel.basic_consume(queue='q1', consumer_tag='tag2')
        self.assertEqual(set(['tag1', 'tag2']), q1.consumers)
        yield channel.basic_cancel('tag2')
        self.assertEqual(set(['tag1']), q1.consumers)
        yield channel.basic_cancel('tag2')
        self.assertEqual(set(['tag1']), q1.consumers)

    @inlineCallbacks
    def test_basic_qos_global_unsupported(self):
        """
        basic_qos() is unsupported with global=True.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.basic_qos(0, 1, True)
        assert False
    test_basic_qos_global_unsupported.skip = "Implement channel exceptions."

    @inlineCallbacks
    def test_basic_qos_per_consumer(self):
        """
        basic_qos() only applies to consumers started after the call.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.queue_declare(queue='q1')
        yield channel.queue_declare(queue='q2')
        self.assertEqual(server_channel.qos_prefetch_count, 0)

        yield channel.basic_consume(queue='q1', consumer_tag='tag1')
        self.assertEqual(server_channel._get_consumer_prefetch('tag1'), 0)

        yield channel.basic_qos(0, 1, False)
        yield channel.basic_consume(queue='q2', consumer_tag='tag2')
        self.assertEqual(server_channel._get_consumer_prefetch('tag1'), 0)
        self.assertEqual(server_channel._get_consumer_prefetch('tag2'), 1)

    @inlineCallbacks
    def test_basic_ack(self):
        """
        basic_ack() should acknowledge a consumed message.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='e1', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_bind(queue='q1', exchange='e1', routing_key='rkey')
        yield channel.basic_consume(queue='q1', consumer_tag='tag1')

        self.assertEqual(len(server_channel.unacked), 0)
        self.broker.basic_publish('e1', 'rkey', 'foo')
        consume_queue = yield channel.client.queue('tag1')
        msg = yield consume_queue.get()
        channel.message_processed()
        self.assertEqual(len(server_channel.unacked), 1)

        yield channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=False)
        yield new_fake_amqp.wait0()
        self.assertEqual(len(server_channel.unacked), 0)

    @inlineCallbacks
    def test_basic_ack_get(self):
        """
        basic_ack() should acknowledge a got message.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='e1', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_bind(queue='q1', exchange='e1', routing_key='rkey')
        self.assertEqual(len(server_channel.unacked), 0)
        self.broker.basic_publish('e1', 'rkey', 'foo')

        yield self.broker.basic_publish('e1', 'rkey', 'blah')
        msg = yield channel.basic_get(queue='q1')
        self.assertEqual(len(server_channel.unacked), 1)

        yield channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=False)
        yield new_fake_amqp.wait0()
        self.assertEqual(len(server_channel.unacked), 0)

    @inlineCallbacks
    def test_basic_ack_consumer_canceled(self):
        """
        basic_ack() should fail if the consumer has been canceled.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.exchange_declare(exchange='e1', type='direct')
        yield channel.queue_declare(queue='q1')
        yield channel.queue_bind(queue='q1', exchange='e1', routing_key='rkey')
        yield channel.basic_consume(queue='q1', consumer_tag='tag1')

        self.assertEqual(len(server_channel.unacked), 0)
        self.broker.basic_publish('e1', 'rkey', 'foo')
        consume_queue = yield channel.client.queue('tag1')
        msg = yield consume_queue.get()
        channel.message_processed()
        self.assertEqual(len(server_channel.unacked), 1)

        yield channel.basic_cancel('tag1')
        yield channel.basic_ack(delivery_tag=msg.delivery_tag, multiple=False)
        yield new_fake_amqp.wait0()
        self.assertEqual(len(server_channel.unacked), 0)

        assert False
    test_basic_ack_consumer_canceled.skip = "Implement channel exceptions."

    @inlineCallbacks
    def test_fake_amqclient(self):
        worker = yield self.get_worker()
        yield worker.pub.publish_json({'message': 'foo'})
        yield worker.conpub.publish_json({'message': 'bar'})
        yield self.broker.wait_delivery()
        self.assertEqual({'message': 'bar'}, worker.msgs[0].payload)

    @inlineCallbacks
    def test_fake_amqclient_qos(self):
        """
        Even if we set QOS, all messages should get delivered.
        """
        worker = yield self.get_worker()
        yield worker.con.channel.basic_qos(0, 1, False)
        yield worker.conpub.publish_json({'message': 'foo'})
        yield worker.conpub.publish_json({'message': 'bar'})
        yield self.broker.wait_delivery()
        self.assertEqual(2, len(worker.msgs))

    @inlineCallbacks
    def test_fake_amqclient_pause(self):
        """
        Pausing and unpausing channels should work as expected.
        """
        worker = yield self.get_worker(paused=True)
        yield worker.conpub.publish_json({'message': 'foo'})
        yield self.broker.wait_delivery()
        self.assertEqual([], worker.msgs)

        yield worker.con.unpause()
        yield self.broker.wait_delivery()
        self.assertEqual(1, len(worker.msgs))
        self.assertEqual({'message': 'foo'}, worker.msgs[0].payload)
        worker.msgs = []

        yield self.broker.wait_delivery()
        yield worker.con.pause()
        yield worker.con.pause()
        yield self.broker.wait_delivery()
        yield worker.conpub.publish_json({'message': 'bar'})
        self.assertEqual([], worker.msgs)

        yield worker.con.unpause()
        yield worker.conpub.publish_json({'message': 'baz'})
        yield self.broker.wait_delivery()
        self.assertEqual(2, len(worker.msgs))
        yield worker.con.unpause()
