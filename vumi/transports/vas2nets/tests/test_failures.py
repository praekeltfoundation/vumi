# encoding: utf-8
from datetime import datetime

from twisted.web import http
from twisted.internet.defer import inlineCallbacks, returnValue, Deferred

from vumi.tests.utils import MockHttpServer
from vumi.transports.failures import (
    FailureMessage, FailureWorker, TemporaryFailure)
from vumi.transports.vas2nets.vas2nets import (
    Vas2NetsTransport, Vas2NetsTransportError)
from vumi.tests.helpers import (
    VumiTestCase, MessageHelper, WorkerHelper, PersistenceHelper)


class FailureCounter(object):
    def __init__(self, count):
        self.count = count
        self.failures = 0
        self.deferred = Deferred()

    def __call__(self):
        self.failures += 1
        if self.failures >= self.count:
            self.deferred.callback(None)


class TestVas2NetsFailureWorker(VumiTestCase):

    @inlineCallbacks
    def setUp(self):
        self.persistence_helper = self.add_helper(PersistenceHelper())
        self.msg_helper = self.add_helper(MessageHelper())
        self.worker_helper = self.add_helper(
            WorkerHelper(self.msg_helper.transport_name))
        self.today = datetime.utcnow().date()

        self.worker = yield self.mk_transport_worker({
            'transport_name': self.msg_helper.transport_name,
            'url': None,
            'username': 'username',
            'password': 'password',
            'owner': 'owner',
            'service': 'service',
            'subservice': 'subservice',
            'web_receive_path': '/receive',
            'web_receipt_path': '/receipt',
            'web_port': 0,
        })
        self.fail_worker = yield self.mk_failure_worker({
            'transport_name': self.msg_helper.transport_name,
            'retry_routing_key': '%(transport_name)s.outbound',
            'failures_routing_key': '%(transport_name)s.failures',
        })

    def mk_transport_worker(self, config):
        config = self.persistence_helper.mk_config(config)
        return self.worker_helper.get_worker(Vas2NetsTransport, config)

    @inlineCallbacks
    def mk_failure_worker(self, config):
        config = self.persistence_helper.mk_config(config)
        worker = yield self.worker_helper.get_worker(
            FailureWorker, config, start=False)
        worker.retry_publisher = yield self.worker.publish_to("foo")
        yield worker.startWorker()
        self.redis = worker.redis
        returnValue(worker)

    @inlineCallbacks
    def mk_mock_server(self, body, headers=None, code=http.OK):
        if headers is None:
            headers = {'X-Nth-Smsid': 'message_id'}

        def handler(request):
            request.setResponseCode(code)
            for k, v in headers.items():
                request.setHeader(k, v)
            return body

        self.mock_server = MockHttpServer(handler)
        self.add_cleanup(self.mock_server.stop)
        yield self.mock_server.start()
        self.worker.config['url'] = self.mock_server.url

    def get_dispatched_failures(self):
        return self.worker_helper.get_dispatched(
            None, 'failures', FailureMessage)

    @inlineCallbacks
    def get_retry_keys(self):
        timestamps = yield self.redis.zrange('retry_timestamps', 0, 0)
        retry_keys = set()
        for timestamp in timestamps:
            bucket_key = "retry_keys." + timestamp
            retry_keys.update((yield self.redis.smembers(bucket_key)))
        returnValue(retry_keys)

    def make_outbound(self, content, **kw):
        kw.setdefault('transport_metadata', {'network_id': 'network-id'})
        return self.msg_helper.make_outbound(content, **kw)

    @inlineCallbacks
    def test_send_sms_success(self):
        yield self.mk_mock_server("Result_code: 00, Message OK")
        msg = self.make_outbound("outbound")
        yield self.worker_helper.dispatch_outbound(msg)
        self.assertEqual(1, len(self.worker_helper.get_dispatched_events()))
        self.assertEqual(0, len(self.get_dispatched_failures()))

    @inlineCallbacks
    def test_send_sms_fail(self):
        """
        A 'No SmsId Header' error should not be retried.
        """
        self.worker.failure_published = FailureCounter(1)
        yield self.mk_mock_server("Result_code: 04, Internal system error "
                                      "occurred while processing message",
                                      {})
        msg = self.make_outbound("outbound")
        yield self.worker_helper.dispatch_outbound(msg)
        yield self.worker.failure_published.deferred
        yield self.worker_helper.kick_delivery()
        self.assertEqual(1, len(self.worker_helper.get_dispatched_events()))
        self.assertEqual(1, len(self.get_dispatched_failures()))

        [twisted_failure] = self.flushLoggedErrors(Vas2NetsTransportError)
        failure = twisted_failure.value
        self.assertTrue("No SmsId Header" in str(failure))

        [fmsg] = self.get_dispatched_failures()
        self.assertTrue(
            "Vas2NetsTransportError: No SmsId Header" in fmsg['reason'])

        [nack] = self.worker_helper.get_dispatched_events()
        self.assertTrue(
            "No SmsId Header" in nack['nack_reason'])

        yield self.worker_helper.kick_delivery()
        [key] = yield self.fail_worker.get_failure_keys()
        self.assertEqual(set(), (yield self.get_retry_keys()))

    @inlineCallbacks
    def test_send_sms_noconn(self):
        """
        A 'connection refused' error should be retried.
        """
        # TODO: Figure out a solution that doesn't require hoping that
        #       nothing's listening on this port.
        self.worker.config['url'] = 'http://127.0.0.1:9999/'

        self.worker.failure_published = FailureCounter(1)
        msg = self.make_outbound("outbound")
        yield self.worker_helper.dispatch_outbound(msg)
        yield self.worker.failure_published.deferred
        self.assertEqual(0, len(self.worker_helper.get_dispatched_events()))
        self.assertEqual(1, len(self.get_dispatched_failures()))

        [twisted_failure] = self.flushLoggedErrors(TemporaryFailure)
        failure = twisted_failure.value
        self.assertTrue("connection refused" in str(failure))

        [fmsg] = self.get_dispatched_failures()
        self.assertEqual(msg.payload, fmsg['message'])
        self.assertEqual(FailureMessage.FC_TEMPORARY,
                         fmsg['failure_code'])
        self.assertTrue(fmsg['reason'].strip().endswith("connection refused"))

        yield self.worker_helper.kick_delivery()
        [key] = yield self.fail_worker.get_failure_keys()
        self.assertEqual(set([key]), (yield self.get_retry_keys()))
