# -*- test-case-name: vumi.database.tests.test_message_io -*-

from vumi.database.base import UglyModel


class ReceivedMessage(UglyModel):
    """
    Store a received message, including all the information we need to
    handle replies, etc.

    We get the following structure from the transport for an incoming sms:
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

    table_name = 'received_messages'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('transport_message_id', 'varchar NOT NULL'),
        ('received_at', 'timestamp with time zone DEFAULT current_timestamp'),
        ('to_msisdn', 'varchar NOT NULL'),
        ('from_msisdn', 'varchar NOT NULL'),
        ('message', 'varchar NOT NULL'),
        ('transport_network_id', 'varchar'),
        ('transport_keyword', 'varchar'), # XXX: ?
        )

    @classmethod
    def receive_message(cls, txn, msg):
        params = {
            'transport_message_id': msg['transport_message_id'],
            'to_msisdn': msg['to_msisdn'],
            'from_msisdn': msg['from_msisdn'],
            'message': msg['message'],
            'transport_network_id': msg.get('transport_network_id'),
            'transport_keyword': msg.get('transport_keyword'),
            }
        txn.execute(cls.insert_values_query(**params), params)
        txn.execute("SELECT lastval()")
        return txn.fetchone()[0]

    @classmethod
    def get_message(cls, txn, msg_id):
        msgs = cls.run_select(txn, "WHERE id=%(id)s", {'id': msg_id})
        if msgs:
            return cls(txn, *msgs[0])
        return None

    @classmethod
    def count_messages(cls, txn):
        return cls.count_rows(txn)


class SentMessage(UglyModel):
    """
    Store a sent message, including data about acks and deliveries.

    This is what we send for an outgoing sms:
    {
        'to_msisdn': '...',
        'from_msisdn': '...',
        'reply_to': 'reply to transport_message_id',
        'id': 'internal message id',
        'transport_network_id': 'MNO unique id, used for number portability',
        'message': 'the body of the sms text'
    }

    This is what we get for an ack:
    {
        'id': 'internal message id',
        'transport_message_id': 'transport message id, alpha numeric'
    }

    This is what we get for a delivery report:
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

    table_name = 'sent_messages'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('sent_at', 'timestamp with time zone DEFAULT current_timestamp'),
        ('from_msisdn', 'varchar NOT NULL'),
        ('to_msisdn', 'varchar NOT NULL'),
        ('reply_to_msg_id', 'integer REFERENCES received_messages'),
        ('message', 'varchar NOT NULL'),
        ('message_send_id', 'varchar NOT NULL'), # TODO: Index this
        ('transport_message_id', 'varchar'), # Filled in after ack
        ('modified_at', 'timestamp with time zone DEFAULT current_timestamp'),
        # TODO: Fill in some fields for delivery reports
        )

    @classmethod
    def get_message(cls, txn, msg_id):
        msgs = cls.run_select(txn, "WHERE id=%(id)s", {'id': msg_id})
        if msgs:
            return cls(txn, *msgs[0])
        return None

    @classmethod
    def count_messages(cls, txn):
        return cls.count_rows(txn)

    @classmethod
    def send_message(cls, txn, msg):
        params = {
            'from_msisdn': msg['from_msisdn'],
            'to_msisdn': msg['to_msisdn'],
            'reply_to_msg_id': msg.get('reply_to_msg_id', None),
            'message': msg['message'],
            'message_send_id': msg['message_send_id'],
            }
        txn.execute(cls.insert_values_query(**params), params)
        txn.execute("SELECT lastval()")
        return txn.fetchone()[0]

    @classmethod
    def ack_message(cls, txn, msg):
        params = {
            'message_send_id': msg['id'],
            'transport_message_id': msg['transport_message_id'],
            }
        query = "UPDATE %s SET %s WHERE %s" % (
            cls.table_name,
            ", ".join([
                    "modified_at=current_timestamp",
                    "transport_message_id=%(transport_message_id)s",
                    ]),
            "message_send_id=%(message_send_id)s")
        txn.execute(query, params)

    @classmethod
    def receive_delivery_report(cls, txn, msg):
        # TODO: Implement this
        pass

