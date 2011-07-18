from datetime import datetime, timedelta
import uuid

from twisted.internet.defer import inlineCallbacks

from vumi.database.tests.test_base import UglyModelTestCase
from vumi.database.message_io import ReceivedMessage, SentMessage
from vumi.campaigns.dispatch import DispatchWorker
from vumi.service import WorkerCreator


class NotNoneMatcher(object):
    def __eq__(self, other):
        return other is not None


def mkinbound(message, from_msisdn=None, to_msisdn=None):
    transport_message_id = uuid.uuid4().get_hex()[10:]
    if not from_msisdn:
        from_msisdn = '+27761234567'
    if not to_msisdn:
        to_msisdn = '12345'
    return {
        'transport_message_id': transport_message_id,
        'transport_timestamp': None, # We don't use this
        'transport_network_id': '007',
        'transport_keyword': None, # Do we get this?
        'to_msisdn': to_msisdn,
        'from_msisdn': from_msisdn,
        'message': message,
        }

def mksend(message, reply_to=None, from_msisdn=None, to_msisdn=None):
    if not from_msisdn:
        from_msisdn = '12345'
    if not to_msisdn:
        to_msisdn = '+27761234567'
    msg = {
        'to_msisdn': to_msisdn,
        'from_msisdn': from_msisdn,
        'message': message,
        }
    if reply_to:
        msg['reply_to_msg_id'] = reply_to
    return msg

def mksent(smsg_dict, message_send_id=None):
    if message_send_id is None:
        message_send_id = NotNoneMatcher()
    return {
        'to_msisdn': smsg_dict['to_msisdn'],
        'from_msisdn': smsg_dict['from_msisdn'],
        'message': smsg_dict['message'],
        'message_send_id': message_send_id,
        'reply_to_msg_id': smsg_dict.get('reply_to_msg_id', None),
        }


# TODO: Generalise this?
# Duplicated in test_load_test.py
class WorkerTestCase(UglyModelTestCase):
    def create_worker(self, worker, config):
        global_options = {
            "hostname": "localhost",
            "port": 5672,
            "username": "vumitest",
            "password": "vumitest",
            "vhost": "/test",
            "specfile": "config/amqp-spec-0-8.xml",
            }
        f = []
        class NoQueueWorkerCreator(WorkerCreator):
            def _connect(self, factory, *_args, **_kw):
                f.append(factory)

        creator = NoQueueWorkerCreator(global_options)
        worker_class = "%s.%s" % (worker.__module__,
                                  worker.__name__)
        creator.create_worker(worker_class, config)
        f = f[0]
        return f.buildProtocol(None)


class StubbedDW(DispatchWorker):
    def dispatch_message(self, message):
        self.dispatched.append(message)

    def setup_dispatch(self):
        pass

    def consume(self, *args, **kw):
        pass

    def publish_to(self, *args, **kw):
        pass

    def send_sms(self, message):
        self.sent_messages.append(message)


class DispatchWorkerTestCase(WorkerTestCase):
    def setUp(self):
        return self.setup_db(ReceivedMessage, SentMessage, dbname='loadtest')

    def tearDown(self):
        return self.shutdown_db()

    def get_dw(self, **kw):
        dw = self.create_worker(StubbedDW, kw)
        dw.dispatched = []
        dw.sent_messages = []
        dw.startWorker()
        return dw

    def assert_message_count(self, expected, model):
        def _txn(txn):
            self.assertEquals(expected, model.count_messages(txn))
        return self.ri(_txn)

    def assert_rmsg_fields(self, msg_dict, msg):
        self.assertEquals(msg_dict['transport_message_id'], msg.transport_message_id)
        self.assertEquals(msg_dict['transport_network_id'], msg.transport_network_id)
        self.assertEquals(msg_dict['to_msisdn'], msg.to_msisdn)
        self.assertEquals(msg_dict['from_msisdn'], msg.from_msisdn)
        self.assertEquals(msg_dict['message'], msg.message)

    def assert_smsg_fields(self, msg_dict, msg):
        self.assertEquals(msg_dict['reply_to_msg_id'], msg.reply_to_msg_id)
        self.assertEquals(msg_dict['message_send_id'], msg.message_send_id)
        self.assertEquals(msg_dict['to_msisdn'], msg.to_msisdn)
        self.assertEquals(msg_dict['from_msisdn'], msg.from_msisdn)
        self.assertEquals(msg_dict['message'], msg.message)

    def mkrmsg(self, *args, **kw):
        def _txn(txn):
            msg_id = ReceivedMessage.receive_message(txn, mkinbound(*args, **kw))
            return ReceivedMessage.get_message(txn, msg_id)
        return self.ri(_txn)

    def mksmsg(self, *args, **kw):
        def _txn(txn):
            smsg = mksend(*args, **kw)
            reply_to = smsg.get('reply_to_msg_id')
            if reply_to:
                rmsg = ReceivedMessage.get_message(txn, reply_to)
                send_id = rmsg.transport_message_id
            else:
                send_id = uuid.uuid4().get_hex()[10:]
            msg_id = SentMessage.send_message(txn, mksent(smsg, send_id))
            return SentMessage.get_message(txn, msg_id)
        return self.ri(_txn)

    @inlineCallbacks
    def test_receive_message(self):
        """
        A received message should be put in the db and dispatched.
        """
        dw = self.get_dw()
        msg_dict = mkinbound('foo')

        yield self.assert_message_count(0, ReceivedMessage)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([], dw.sent_messages)

        yield dw.process_message(msg_dict.copy())

        yield self.assert_message_count(1, ReceivedMessage)
        msg = yield self.ri(ReceivedMessage.get_message, 1)
        self.assert_rmsg_fields(msg_dict, msg)

        dispatched = msg_dict.copy()
        dispatched['msg_id'] = 1
        self.assertEquals([dispatched], dw.dispatched)
        self.assertEquals([], dw.sent_messages)

    @inlineCallbacks
    def test_send_message(self):
        """
        A sent message should be put in the db and sent.
        """
        dw = self.get_dw()
        msg_dict = mksend('foo')

        yield self.assert_message_count(0, SentMessage)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([], dw.sent_messages)

        yield dw.process_send(msg_dict.copy())

        yield self.assert_message_count(1, SentMessage)
        msg = yield self.ri(SentMessage.get_message, 1)
        smsg_dict = mksent(msg_dict)
        self.assert_smsg_fields(smsg_dict, msg)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([smsg_dict], dw.sent_messages)

    @inlineCallbacks
    def test_send_reply_message(self):
        """
        A sent reply message should be put in the db and sent with
        appropriate references.
        """
        rmsg = yield self.mkrmsg('bar')

        dw = self.get_dw()
        msg_dict = mksend('foo', 1)

        yield self.assert_message_count(0, SentMessage)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([], dw.sent_messages)

        yield dw.process_send(msg_dict.copy())

        yield self.assert_message_count(1, SentMessage)
        msg = yield self.ri(SentMessage.get_message, 1)
        smsg_dict = mksent(msg_dict, rmsg.transport_message_id)
        self.assert_smsg_fields(smsg_dict, msg)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([smsg_dict], dw.sent_messages)


    @inlineCallbacks
    def test_ack(self):
        """
        An ack should update the db appropriately.
        """
        rmsg = yield self.mkrmsg('bar')
        smsg = yield self.mksmsg('foo')
        ack_dict = {
            'id': smsg.message_send_id,
            'transport_message_id': uuid.uuid4().get_hex()[10:],
            }
        dw = self.get_dw()

        yield self.assert_message_count(1, SentMessage)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([], dw.sent_messages)
        self.assertEquals(None, smsg.transport_message_id)

        yield dw.process_ack(ack_dict.copy())

        yield self.assert_message_count(1, SentMessage)
        msg = yield self.ri(SentMessage.get_message, 1)
        self.assertEquals(ack_dict['transport_message_id'], msg.transport_message_id)
        self.assertEquals([], dw.dispatched)
        self.assertEquals([], dw.sent_messages)
