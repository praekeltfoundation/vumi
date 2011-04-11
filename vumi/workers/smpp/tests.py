import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'vumi.webapp.settings'


from twisted.trial.unittest import TestCase
from twisted.python import log
from vumi.utils import TestPublisher
from vumi.message import Message
from vumi.workers.smpp.worker import SMSBatchConsumer

from django.conf import settings

from django.contrib.auth.models import User
from vumi.webapp.api.models import SentSMSBatch
from vumi.utils import setup_django_test_database, teardown_django_test_database

class SMSBatchConsumerTestCase(TestCase):

    def setUp(self):
        self.publisher = TestPublisher()
        self.debatcher = SMSBatchConsumer(self.publisher)
        self.runner, self.config = setup_django_test_database()
    
    def tearDown(self):
        teardown_django_test_database(self.runner, self.config)
    
    def test_debatching(self):
        """It should publish an incoming batch of 3 into separate
        individual messages"""
        
        user = User.objects.create(username='vumi')
        batch = SentSMSBatch(user=user, title='test batch')
        # create 3 test messages in this batch
        recipients = [u'27123456789%s' % i for i in range(0,3)]
        for recipient in recipients:
            batch.sentsms_set.create(to_msisdn=recipient,
                from_msisdn='27123456789', message='testing message',
                transport_name='transport', user=user)
            
        message = Message(kwargs={
            "id": batch.pk
        })

        self.debatcher.consume_message(message)
        self.assertEquals(len(self.publisher.queue), 3)
        self.assertEquals(set([m.payload['to_msisdn'] for m,kwargs in
                self.publisher.queue]),
                set(recipients))
        self.assertTrue(all(kwargs['routing_key'] == 'sms.outbound.transport'
            for m,kwargs in self.publisher.queue))

    
