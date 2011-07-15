# -*- test-case-name: vumi.database.tests.test_message_io -*-

from vumi.database.base import UglyModel


class ReceivedMessage(UglyModel):
    table_name = 'received_messages'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('received', 'timestamp with time zone DEFAULT current_timestamp'),
        ('from_msisdn', 'varchar NOT NULL'),
        ('to_msisdn', 'varchar NOT NULL'),
        ('message', 'varchar NOT NULL'),
        ('destination', 'varchar'),
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
        return cls.count_rows(txn)

    @classmethod
    def dispatch_message(cls, txn, msg_id, destination):
        msg = cls.get_message(txn, msg_id)
        if not msg:
            raise ValueError("Can't find message %s to dispatch" % (msg_id,))
        query = "UPDATE %s SET destination=%%(destination)s WHERE id=%%(id)s" % (cls.table_name,)
        txn.execute(query, {'id': msg_id, 'destination': destination})



class SentMessage(UglyModel):
    table_name = 'sent_message'
    fields = (
        ('id', 'SERIAL PRIMARY KEY'),
        ('sent', 'timestamp with time zone DEFAULT current_timestamp'),
        ('from_msisdn', 'varchar NOT NULL'),
        ('to_msisdn', 'varchar NOT NULL'),
        ('message', 'varchar NOT NULL'),
        ('acknowledged', 'boolean DEFAULT false'),         #)
        ('delivered', 'boolean DEFAULT false'),            #\ Do we want these?
        ('acknowledged_time', 'timestamp with time zone'), #/
        ('delivered_time', 'timestamp with time zone'),    #)
        )

    @classmethod
    def send_message(cls, txn, msg):
        raise NotImplementedError()
