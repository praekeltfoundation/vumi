# -*- test-case-name: vumi.campaigns.tests.test_dispatch -*-

import uuid

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.database.message_io import ReceivedMessage, SentMessage
from vumi.campaigns.base import DatabaseWorker


class DispatchWorker(DatabaseWorker):

    @inlineCallbacks
    def setup_worker(self):
        # Incoming SMS rkey: sms.inbound.<transport-name>.<to_msisdn>
        self.transport = self.get_config('transport')
        self.shortcode = self.get_config('shortcode')
        inbound_rkey = "sms.inbound.%s.%s" % (self.transport, self.shortcode)
        outbound_rkey = "sms.outbound.%s" % (self.transport,)
        ack_rkey = "sms.ack.%s" % (self.transport,)
        delivery_rkey = "sms.receipt.%s" % (self.transport,)
        self.ack_consumer = yield self.consume(ack_rkey, self.consume_ack)
        self.delivery_consumer = yield self.consume(delivery_rkey,
                                                    self.consume_delivery)
        self.outbound_publisher = yield self.publish_to(outbound_rkey)
        self.inbound_consumer = yield self.consume(inbound_rkey,
                                                   self.consume_message)
        yield self.setup_dispatch()

    def setup_dispatch(self):
        raise NotImplementedError()

    def consume_ack(self, message):
        return self.process_ack(message.payload)

    def consume_delivery(self, message):
        return self.process_delivery(message.payload)

    def ri(self, *args, **kw):
        return self.db.runInteraction(*args, **kw)

    @inlineCallbacks
    def process_message(self, message):
        """
        Receive an incoming SMS.

        The message structure looks like this:
        {
            'transport_message_id': 'alpha numeric',
            'transport_timestamp': 'iso 8601 format',
            'transport_network_id': 'MNO unique id, used for number portability',
            'transport_keyword': 'keyword if provided by vas2nets',
            'to_msisdn': '+27761234567',
            'from_msisdn': '+27761234567',
            'message': 'message content'
        }
        """
        log.msg("Received sms: %s" % (message))
        msg_id = yield self.ri(ReceivedMessage.receive_message, message)
        message['msg_id'] = msg_id
        yield self.dispatch_message(message)

    def dispatch_message(self, message):
        raise NotImplementedError()

    # TODO: Figure out what to do about the message ids.
    def process_ack(self, message):
        """
        Receive an ack for an outgoing message.

        The message structure looks like this:
        {
            'id': 'internal message id',
            'transport_message_id': 'transport message id, alpha numeric'
        }
        """
        return self.ri(SentMessage.ack_message, message['id'],
                       message['transport_message_id'])

    def process_delivery(self, message):
        """
        Receive a delivery report for an outgoing message.

        The message structure looks like this:
        {
            'transport_message_id': 'alpha numeric',
            'transport_status': 'numeric',
            'transport_status_message': 'text status accompanying numeric status',
            'transport_timestamp': 'iso 8601 format',
            'transport_network_id': 'MNO unique id, used for number portability',
            'to_msisdn': '+27761234567',
            'id': 'transport message id if this was a reply, else internal id'
        }
        """
        pass

    @inlineCallbacks
    def process_send(self, message):
        """
        Send an outgoing sms.

        The message structure looks like this:
        {
            'to_msisdn': '...',
            'from_msisdn': '...',
            'reply_to': 'reply to transport_message_id',
            'id': 'internal message id',
            'transport_network_id': 'MNO unique id, used for number portability',
            'message': 'the body of the sms text'
        }
        """
        reply_to = message.get('reply_to_msg_id', None)
        if reply_to:
            msg = yield self.ri(ReceivedMessage.get_message, reply_to)
            send_message_id = msg.transport_message_id
        else:
            send_message_id = uuid.uuid4().get_hex()[10:]

        out_msg = {
            'to_msisdn': message['to_msisdn'],
            'from_msisdn': message['from_msisdn'],
            'message_send_id': send_message_id,
            'reply_to_msg_id': reply_to,
            'message': message['message'],
            }
        yield self.ri(SentMessage.send_message, out_msg)

        yield self.send_sms(out_msg)

    def send_sms(self, message):
        msg = message.copy()
        msg['id'] = msg.pop('message_send_id')
        msg.pop('reply_to_msg_id')
        pass
