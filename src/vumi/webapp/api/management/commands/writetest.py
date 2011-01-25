from django.core.management.base import BaseCommand

from datetime import datetime, timedelta

from vumi.webapp.api import forms

class Command(BaseCommand):
    args = '<count>'
    help = 'Times ORM inserts'

    def handle(self, *args, **options):
        count = 0
        if len(args) > 0:
            count = int(args[0])
        print "COUNT:", count
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
            #print send_sms.pk

        delta = datetime.now() - start
        print 'time to write messages =', delta

