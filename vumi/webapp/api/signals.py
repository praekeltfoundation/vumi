from django.dispatch import Signal
from vumi.webapp.api.models import Profile
from vumi.webapp.api.tasks import (SendSMSTask, ReceiveSMSTask,
                                   DeliveryReportTask, SendSMSBatchTask)

# custom signals for the api
sms_batch_scheduled = Signal(providing_args=['instance', 'pk'])
sms_scheduled = Signal(providing_args=['instance', 'pk'])
sms_sent = Signal(providing_args=['instance', 'pk'])
sms_received = Signal(providing_args=['instance', 'pk'])
sms_receipt = Signal(providing_args=['instance', 'pk', 'receipt'])


def sms_scheduled_handler(*args, **kwargs):
    sms_scheduled_worker(kwargs['instance'])


def sms_scheduled_worker(sent_sms):
    """Responsibile for delivering of SMSs"""
    SendSMSTask.delay(pk=sent_sms.pk)


def sms_batch_scheduled_handler(*args, **kwargs):
    sms_batch_scheduled_worker(kwargs['instance'])


def sms_batch_scheduled_worker(batch):
    """Responsible for scheduling jobs for the debatcher"""
    SendSMSBatchTask.delay(pk=batch.pk)


def sms_received_handler(*args, **kwargs):
    sms_received_worker(kwargs['instance'])


def sms_received_worker(received_sms):
    """Responsible for dealing with received SMSs"""
    ReceiveSMSTask.delay(pk=received_sms.pk)


def sms_receipt_handler(*args, **kwargs):
    sms_receipt_worker(kwargs['instance'], kwargs['receipt'])


def sms_receipt_worker(sent_sms, receipt):
    """Responsible for dealing with received SMS delivery receipts"""
    DeliveryReportTask.delay(pk=sent_sms.pk, receipt=receipt)


def create_profile_handler(*args, **kwargs):
    if kwargs['created']:
        create_profile_worker(kwargs['instance'])


def create_profile_worker(user):
    """Automatically create a profile for a newly created user"""
    Profile.objects.create(user=user)


from vumi.webapp.api.models import SentSMS, ReceivedSMS, SentSMSBatch
sms_batch_scheduled.connect(sms_batch_scheduled_handler, sender=SentSMSBatch)
sms_scheduled.connect(sms_scheduled_handler, sender=SentSMS)
sms_received.connect(sms_received_handler, sender=ReceivedSMS)
sms_receipt.connect(sms_receipt_handler, sender=SentSMS)

from django.db.models.signals import post_save
from django.contrib.auth.models import User
post_save.connect(create_profile_handler, sender=User)
