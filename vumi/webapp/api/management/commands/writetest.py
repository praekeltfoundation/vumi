from django.core.management.base import BaseCommand
from django.db import connection, transaction

from datetime import datetime, timedelta

from vumi.webapp.api import forms

class Command(BaseCommand):
    args = '<count>'
    help = 'Times ORM inserts'


    def handle(self, *args, **kwargs):
        count = 0
        if len(args) > 0:
            count = int(args[0])
        print "COUNT:", count
        batch = self.by_orm(count)
        self.by_custom_sql(count, batch)


    def by_orm(self, count):
        start = datetime.now()

        form1 = forms.SentSMSBatchForm({'title':'writetest', 'user':1})
        if not form1.is_valid():
            raise FormValidationError(form1)
        batch = form1.save()
        for x in range(count):
            dictionary = {
                    'transport_name': 'smpp',
                    'from_msisdn': u'27123456789',
                    'batch': batch.pk,
                    'user': 1,
                    'to_msisdn': u'27123456789',
                    'message': u'hello world',
                    }
            form2 = forms.SentSMSForm(dictionary)
            if not form2.is_valid():
                raise FormValidationError(form2)
            send_sms = form2.save()
            #print connection.queries
            #print send_sms.pk

        delta = datetime.now() - start
        print 'time to write messages =', delta
        return batch

    def by_custom_sql(self, count, batch):
        start = datetime.now()

        cursor = connection.cursor()
        for x in range(count):
            cursor.execute("""
            INSERT INTO "api_sentsms" (
                "user_id",
                "batch_id",
                "to_msisdn",
                "from_msisdn",
                "charset",
                "message",
                "transport_name",
                "transport_msg_id",
                "transport_status",
                "created_at",
                "updated_at",
                "delivered_at"
            )
            VALUES (
                1,
                %s,
                E\'27123456789\',
                E\'27123456789\',
                E\'\',
                E\'XXX\',
                E\'smpp\',
                E\'\',
                E\'\',
                %s,
                %s,
                NULL
            )
            """, [batch.id, batch.created_at, batch.updated_at])
        transaction.commit_unless_managed()

        delta = datetime.now() - start
        print 'time to write custom sql =', delta
