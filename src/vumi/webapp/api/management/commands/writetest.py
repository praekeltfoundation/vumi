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
        send_group = self.by_orm(count)
        self.by_custom_sql(count, send_group)


    def by_orm(self, count):
        start = datetime.now()

        form1 = forms.SendGroupForm({'title':'writetest', 'user':1})
        if not form1.is_valid():
            raise FormValidationError(form1)
        send_group = form1.save()
        for x in range(count):
            dictionary = {
                    'transport_name': 'smpp',
                    'from_msisdn': u'27123456789',
                    'send_group': send_group.id,
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
        return send_group

    def by_custom_sql(self, count, send_group):
        start = datetime.now()

        cursor = connection.cursor()
        for x in range(count):
            cursor.execute("""
            INSERT INTO "api_sentsms" (
                "user_id",
                "send_group_id",
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
            """, [send_group.id, send_group.created_at, send_group.updated_at])
        transaction.commit_unless_managed()

        delta = datetime.now() - start
        print 'time to write custom sql =', delta
