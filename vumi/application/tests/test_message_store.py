# -*- coding: utf-8 -*-

"""Tests for vumi.application.message_store."""

from vumi.message import TransportEvent
from vumi.tests.utils import FakeRedis
from vumi.application.tests.test_base import ApplicationTestCase
from vumi.application.message_store import MessageStore


class TestMessageStore(ApplicationTestCase):
    # inherits from ApplicationTestCase for .mkmsg_in and .mkmsg_out

    def setUp(self):
        self.r_server = FakeRedis()
        self.store = MessageStore(self.r_server, 'teststore')

    def tearDown(self):
        self.r_server.teardown()

    def test_batch_start(self):
        tag1 = ("poolA", "tag1")
        batch_id = self.store.batch_start([tag1])
        self.assertEqual(self.store.batch_messages(batch_id), [])
        self.assertEqual(self.store.batch_status(batch_id), {
            'ack': 0, 'delivery_report': 0, 'message': 0, 'sent': 0,
            })
        self.assertEqual(self.store.batch_common(batch_id),
                         {"tags": [tag1]})
        self.assertEqual(self.store.tag_common(tag1),
                         {"current_batch_id": batch_id})

    def test_batch_done(self):
        tag1 = ("poolA", "tag1")
        batch_id = self.store.batch_start([tag1])
        self.store.batch_done(batch_id)
        self.assertEqual(self.store.batch_common(batch_id),
                         {"tags": [tag1]})
        self.assertEqual(self.store.tag_common(tag1),
                         {"current_batch_id": None})

    def test_add_message(self):
        batch_id = self.store.batch_start([("pool", "tag")])
        msg = self.mkmsg_out(content="outfoo")
        msg_id = msg['message_id']
        self.store.add_message(batch_id, msg)

        self.assertEqual(self.store.get_message(msg_id), msg)
        self.assertEqual(self.store.message_batches(msg_id), [batch_id])
        self.assertEqual(self.store.batch_messages(batch_id), [msg_id])
        self.assertEqual(self.store.message_events(msg_id), [])
        self.assertEqual(self.store.batch_status(batch_id), {
            'ack': 0, 'delivery_report': 0, 'message': 1, 'sent': 1,
            })

    def test_add_ack_event(self):
        batch_id = self.store.batch_start([("pool", "tag")])
        msg = self.mkmsg_out(content="outfoo")
        msg_id = msg['message_id']
        ack = TransportEvent(user_message_id=msg_id, event_type='ack',
                             sent_message_id='xyz')
        ack_id = ack['event_id']
        self.store.add_message(batch_id, msg)
        self.store.add_event(ack)

        self.assertEqual(self.store.get_event(ack_id), ack)
        self.assertEqual(self.store.message_events(msg_id), [ack_id])

    def test_add_inbound_message(self):
        msg = self.mkmsg_in(content="infoo")
        msg_id = msg['message_id']
        self.store.add_inbound_message(msg)

        self.assertEqual(self.store.get_inbound_message(msg_id), msg)

    def test_add_inbound_message_with_tag(self):
        batch_id = self.store.batch_start([("pool1", "default10001")])
        msg = self.mkmsg_in(content="infoo", to_addr="+1234567810001",
                            transport_type="sms")
        msg['tag'] = ["pool1", "default10001"]
        msg_id = msg['message_id']
        self.store.add_inbound_message(msg)

        self.assertEqual(self.store.get_inbound_message(msg_id), msg)
        self.assertEqual(self.store.batch_replies(batch_id), [msg_id])
