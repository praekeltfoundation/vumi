# encoding: utf-8
from datetime import datetime

from twisted.web import http
from twisted.web.resource import Resource
from twisted.trial import unittest
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from vumi.message import TransportUserMessage, from_json
from vumi.tests.utils import get_stubbed_worker, FakeRedis, TestResourceWorker
from vumi.tests.fake_amqp import FakeAMQPBroker
from vumi.transports.failures import FailureMessage, FailureWorker
from vumi.transports.vas2nets.vas2nets import Vas2NetsTransport


class BadVas2NetsResource(Resource):
    isLeaf = True

    def __init__(self, body, headers=None, code=http.OK):
        self.body = body
        self.code = code
        if headers is None:
            headers = {'X-Nth-Smsid': 'message_id'}
        self.headers = headers

    def render_POST(self, request):
        request.setResponseCode(self.code)
        for k, v in self.headers.items():
            request.setHeader(k, v)
        return self.body


class FailureCounter(object):
    def __init__(self, count):
        self.count = count
        self.failures = 0
        self.deferred = Deferred()

    def __call__(self):
        self.failures += 1
        if self.failures >= self.count:
            self.deferred.callback(None)


class Vas2NetsFailureWorkerTestCase(unittest.TestCase):

    @inlineCallbacks
    def setUp(self):
        self.today = datetime.utcnow().date()
        self.port = 9999
        self.path = '/api/v1/sms/vas2nets/receive/'
        self.config = {
            'transport_name': 'vas2nets',
            'url': 'http://localhost:%s%s' % (self.port, self.path),
            'username': 'username',
            'password': 'password',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice',
            'web_receive_path': '/receive',
            'web_receipt_path': '/receipt',
            'web_port': 9998,
        }
        self.fail_config = {
            'transport_name': 'vas2nets',
            'retry_routing_key': '%(transport_name)s.outbound',
            'failures_routing_key': '%(transport_name)s.failures',
            }
        self.workers = []
        self.broker = FakeAMQPBroker()
        self.redis = FakeRedis()
        self.worker = yield self.mk_transport_worker(self.config, self.broker)
        self.fail_worker = yield self.mk_failure_worker(
            self.fail_config, self.broker, self.redis)

    def tearDown(self):
        for worker in self.workers:
            worker.stopWorker()

    @inlineCallbacks
    def mk_transport_worker(self, config, broker):
        worker = get_stubbed_worker(Vas2NetsTransport, config, broker)
        self.workers.append(worker)
        yield worker.startWorker()
        returnValue(worker)

    @inlineCallbacks
    def mk_failure_worker(self, config, broker, redis):
        w = get_stubbed_worker(FailureWorker, config, broker)
        self.workers.append(w)
        yield w.startWorker()
        w.retry_publisher = yield self.worker.publish_to("foo")
        w.r_server = redis
        returnValue(w)

    @inlineCallbacks
    def mk_resource_worker(self, body, headers=None, code=http.OK):
        w = get_stubbed_worker(TestResourceWorker, {}, self.broker)
        self.workers.append(w)
        w.set_resources([(self.path, BadVas2NetsResource,
                          (body, headers, code))])
        yield w.startWorker()
        returnValue(w)

    def get_dispatched(self, rkey):
        return self.broker.get_dispatched('vumi', rkey)

    def get_retry_keys(self):
        timestamps = self.redis.zrange(
            self.fail_worker._retry_timestamps_key, 0, 0)
        retry_keys = set()
        for timestamp in timestamps:
            bucket_key = self.fail_worker.r_key("retry_keys." + timestamp)
            retry_keys.update(self.redis.smembers(bucket_key))
        return retry_keys

    def mkmsg_out(self, in_reply_to=None):
        return TransportUserMessage(
            to_addr='+41791234567',
            from_addr='9292',
            message_id='1',
            transport_name='vas2nets',
            transport_type='sms',
            transport_metadata={
               'network_id': 'network-id',
               },
            content='hello world',
            in_reply_to=in_reply_to,
            )

    def assert_dispatched_count(self, count, routing_key):
        self.assertEqual(count, len(self.get_dispatched(routing_key)))

    @inlineCallbacks
    def test_send_sms_success(self):
        yield self.mk_resource_worker("Result_code: 00, Message OK")
        yield self.worker._process_message(self.mkmsg_out())
        self.assert_dispatched_count(1, 'vas2nets.event')
        self.assert_dispatched_count(0, 'vas2nets.failures')

    @inlineCallbacks
    def test_send_sms_fail(self):
        """
        A 'No SmsId Header' error should not be retried.
        """
        self.worker.failure_published = FailureCounter(1)
        yield self.mk_resource_worker("Result_code: 04, Internal system error "
                                      "occurred while processing message",
                                      {})
        yield self.worker._process_message(self.mkmsg_out())
        yield self.worker.failure_published.deferred
        yield self.broker.kick_delivery()
        self.assert_dispatched_count(0, 'vas2nets.event')
        self.assert_dispatched_count(1, 'vas2nets.failures')
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = from_json(fmsg.body)
        self.assertTrue(
            "Vas2NetsTransportError: No SmsId Header" in fmsg['reason'])

        yield self.broker.kick_delivery()
        [key] = self.fail_worker.get_failure_keys()
        self.assertEqual(set(), self.get_retry_keys())

    @inlineCallbacks
    def test_send_sms_noconn(self):
        """
        A 'connection refused' error should be retried.
        """
        self.worker.failure_published = FailureCounter(1)
        msg = self.mkmsg_out()
        yield self.worker._process_message(msg)
        yield self.worker.failure_published.deferred
        self.assert_dispatched_count(0, 'vas2nets.event')
        self.assert_dispatched_count(1, 'vas2nets.failures')
        [fmsg] = self.get_dispatched('vas2nets.failures')
        fmsg = from_json(fmsg.body)
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertEqual(FailureMessage.FC_TEMPORARY,
                         fmsg['failure_code'])
        self.assertTrue(fmsg['reason'].strip().endswith("connection refused"))

        yield self.broker.kick_delivery()
        [key] = self.fail_worker.get_failure_keys()
        self.assertEqual(set([key]), self.get_retry_keys())
