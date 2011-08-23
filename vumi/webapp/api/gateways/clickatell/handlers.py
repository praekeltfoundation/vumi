import logging
from datetime import datetime

from piston.handler import BaseHandler
from piston.utils import rc, throttle, validate
from piston.utils import FormValidationError

from vumi.webapp.api.models import SentSMS, ReceivedSMS
from vumi.webapp.api import forms
from vumi.webapp.api import signals


class SMSReceiptHandler(BaseHandler):
    allowed_methods = ('POST',)

    @throttle(6000, 60)  # allow for 100 a second
    @validate(forms.SMSReceiptForm)
    def create(self, request):
        logging.debug('Got notified of a delivered SMS to: %s' %
                      (request.POST['to']),)
        try:
            pk = int(request.POST['cliMsgId'])
            transport_msg_id = request.POST['apiMsgId']
            transport_status = request.POST['status']
            timestamp = float(request.POST['timestamp'])

            sms = SentSMS.objects.get(id=pk,
                                      transport_name='Clickatell',
                                      transport_msg_id=transport_msg_id)
            sms.user = request.user
            sms.transport_status = transport_status
            sms.delivery_at = datetime.utcfromtimestamp(timestamp)
            sms.save()

            signals.sms_receipt.send(sender=SentSMS, instance=sms,
                                     pk=sms.pk, receipt=request.POST.copy())

            return rc.CREATED
        except SentSMS.DoesNotExist:
            return rc.NOT_FOUND


class ReceiveSMSHandler(BaseHandler):
    allowed_methods = ('POST',)
    model = ReceivedSMS
    exclude = ('user',)

    @throttle(6000, 60)  # allow for 100 a second
    def create(self, request):
        form = forms.ReceivedSMSForm({
            'user': request.user.pk,
            'to_msisdn': request.POST.get('to'),
            'from_msisdn': request.POST.get('from'),
            'message': request.POST.get('text'),
            'transport_name': 'Clickatell',
            'transport_msg_id': request.POST.get('api_id'),
            'received_at': datetime.strptime(request.POST.get('timestamp'),
                                             "%Y-%m-%d %H:%M:%S")
        })
        if not form.is_valid():
            raise FormValidationError(form)

        receive_sms = form.save()
        logging.debug('Receiving an SMS from: %s' % receive_sms.from_msisdn)
        signals.sms_received.send(sender=ReceivedSMS, instance=receive_sms,
                                  pk=receive_sms.pk)
        return receive_sms
