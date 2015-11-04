from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.amqp_service import AMQPClientService
from vumi.service import Worker
from vumi.tests.fake_amqp import FakeAMQPBroker, make_fake_server
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
        self.broker = FakeAMQPBroker()
        self.add_cleanup(self.broker.wait_delivery)

    def make_service(self, broker=None):
        if broker is None:
            broker = self.broker
        fake_server = make_fake_server(broker)
        service = AMQPClientService(fake_server.endpoint)
        service.startService()
        return service.await_connected()

    @inlineCallbacks
    def make_channel(self, service=None):
        if service is None:
            service = yield self.make_service()
        channel = yield service.get_channel()
        returnValue(
            (channel, service._fake_server.channels[channel.channel_number]))

    @inlineCallbacks
    def get_worker(self, **config):
        worker = ToyWorker({}, config)
        worker._amqp_client = yield self.make_service()
        yield worker.startWorker()
        returnValue(worker)

    @inlineCallbacks
    def test_channel_open(self):
        """
        channel_open() creates a new channel.
        """
        service = yield self.make_service()
        self.assertEqual(service._fake_server.channels, {})
        channel = yield service.get_channel()
        self.assertEqual(channel.channel_number, 1)
        self.assertEqual(service._fake_server.channels.keys(), [1])
        channel = yield service.get_channel()
        self.assertEqual(channel.channel_number, 2)
        self.assertEqual(sorted(service._fake_server.channels.keys()), [1, 2])

    @inlineCallbacks
    def test_channel_close(self):
        """
        channel_close() closes a channel.
        """
        service = yield self.make_service()
        self.assertEqual(service._fake_server.channels, {})
        channel = yield service.get_channel()
        self.assertEqual(channel.channel_number, 1)
        self.assertEqual(service._fake_server.channels.keys(), [1])

        yield channel.close()
        yield self.broker.wait0()
        self.assertEqual(service._fake_server.channels, {})

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
        _, method, _, body = yield channel.basic_get(queue='q1')
        self.assertEqual(method.NAME, 'Basic.GetOk')
        self.assertEqual(body, 'blah')
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

        _, method, _, body = yield channel.basic_get(queue='q1')
        self.assertEqual(method.NAME, 'Basic.GetEmpty')
        self.assertEqual(body, None)
        self.assertEqual(server_channel.unacked, [])

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
        yield self.broker.wait0()
        self.assertEqual(set(['tag1']), q1.consumers)
        yield channel.basic_consume(queue='q1', consumer_tag='tag2')
        yield self.broker.wait0()
        self.assertEqual(set(['tag1', 'tag2']), q1.consumers)
        yield channel.basic_cancel(consumer_tag='tag2')
        yield self.broker.wait0()
        self.assertEqual(set(['tag1']), q1.consumers)
        yield channel.basic_cancel(consumer_tag='tag2')
        yield self.broker.wait0()
        self.assertEqual(set(['tag1']), q1.consumers)

    @inlineCallbacks
    def test_basic_qos_global_unsupported(self):
        """
        basic_qos() is unsupported with global=True.
        """
        channel, server_channel = yield self.make_channel()
        yield channel.basic_qos(
            prefetch_size=0, prefetch_count=1, all_channels=True)
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
        yield self.broker.wait0()
        self.assertEqual(server_channel._get_consumer_prefetch('tag1'), 0)

        yield channel.basic_qos(
            prefetch_size=0, prefetch_count=1, all_channels=False)
        yield channel.basic_consume(queue='q2', consumer_tag='tag2')
        yield self.broker.wait0()
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
        consume_queue, ctag = yield channel.basic_consume(queue='q1')

        self.assertEqual(len(server_channel.unacked), 0)
        self.broker.basic_publish('e1', 'rkey', 'foo')
        _, props, _, body = yield consume_queue.get()
        yield channel._fake_channel.message_processed()
        self.assertEqual(len(server_channel.unacked), 1)

        yield channel.basic_ack(
            delivery_tag=props.delivery_tag, multiple=False)
        yield self.broker.wait0()
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
        _, props, _, body = yield channel.basic_get(queue='q1')
        self.assertEqual(len(server_channel.unacked), 1)

        yield channel.basic_ack(
            delivery_tag=props.delivery_tag, multiple=False)
        yield self.broker.wait0()
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
        consume_queue, consumer_tag = yield channel.basic_consume(queue='q1')

        self.assertEqual(len(server_channel.unacked), 0)
        self.broker.basic_publish('e1', 'rkey', 'foo')
        _, props, _, body = yield consume_queue.get()
        yield channel._fake_channel.message_processed()
        self.assertEqual(len(server_channel.unacked), 1)

        yield channel.basic_cancel(consumer_tag)
        yield channel.basic_ack(
            delivery_tag=props.delivery_tag, multiple=False)
        yield self.broker.wait0()
        self.assertEqual(len(server_channel.unacked), 0)

        assert False
    test_basic_ack_consumer_canceled.skip = "Implement channel exceptions."

    @inlineCallbacks
    def test_fake_amqservice(self):
        worker = yield self.get_worker()
        yield worker.pub.publish_json({'message': 'foo'})
        yield worker.conpub.publish_json({'message': 'bar'})
        yield self.broker.wait_delivery()
        self.assertEqual({'message': 'bar'}, worker.msgs[0].payload)

    @inlineCallbacks
    def test_fake_amqservice_qos(self):
        """
        Even if we set QOS, all messages should get delivered.
        """
        worker = yield self.get_worker()
        yield worker.con.channel.basic_qos(
            prefetch_size=0, prefetch_count=1, all_channels=False)
        yield worker.conpub.publish_json({'message': 'foo'})
        yield worker.conpub.publish_json({'message': 'bar'})
        yield self.broker.wait_delivery()
        self.assertEqual(2, len(worker.msgs))

    @inlineCallbacks
    def test_fake_amqservice_pause(self):
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

    @inlineCallbacks
    def test_delayed_setup(self):
        """
        The server connection can be delayed to test client readiness handling.
        """
        broker = FakeAMQPBroker(delay_server=True)
        service_d = self.make_service(broker)
        yield self.broker.wait0()
        [server] = broker.server_protocols
        self.assertEqual(server.conn_state, "CONNECTING_DELAYED")
        self.assertNoResult(service_d)
        yield server.finish_connecting()
        self.assertEqual(server.conn_state, "CONNECTED")
        self.successResultOf(service_d)
