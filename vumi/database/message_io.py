# -*- test-case-name: vumi.database.tests.test_message_io -*-

from vumi.database.base import UglyModel


class ReceivedMessage(UglyModel):
    table_name = 'received_message'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('received', 'timestamp DEFAULT current_timestamp'),
        ('from_msisdn', 'varchar NOT NULL'),
        ('to_msisdn', 'varchar NOT NULL'),
        ('message', 'varchar NOT NULL'),
        )

    @classmethod
    def receive_message(cls, txn, msg):
        params = {
            'from_msisdn': msg['from_msisdn'],
            'to_msisdn': msg['to_msisdn'],
            'message': msg['message'],
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
        txn.execute("SELECT count(1) FROM %s" % (cls.table_name,))
        return txn.fetchone()[0]


class SentMessage(UglyModel):
    table_name = 'received_message'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('sent', 'timestamp DEFAULT current_timestamp'),
        ('from_msisdn', 'varchar NOT NULL'),
        ('to_msisdn', 'varchar NOT NULL'),
        ('message', 'varchar NOT NULL'),
        ('acknowledged', 'boolean DEFAULT false'),       #)
        ('delivered', 'boolean DEFAULT false'),          #\ Do we want this?
        ('acknowledged_time', 'timestamp DEFAULT NULL'), #/
        ('delivered_time', 'timestamp DEFAULT NULL'),    #)
        )

    @classmethod
    def send_message(cls, txn, msg):
        raise NotImplementedError()
