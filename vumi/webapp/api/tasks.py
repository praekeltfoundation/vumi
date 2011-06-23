from celery.task import Task


class SendSMSTask(Task):
    routing_key = 'vumi.webapp.sms.send'


class SendSMSBatchTask(Task):
    routing_key = 'sms.internal.debatcher'


class ReceiveSMSTask(Task):
    routing_key = 'vumi.webapp.sms.receive'


class DeliveryReportTask(Task):
    routing_key = 'vumi.webapp.sms.receipt'
