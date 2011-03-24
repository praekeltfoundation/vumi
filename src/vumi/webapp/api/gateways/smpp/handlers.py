import re, yaml, logging
from datetime import datetime, timedelta

from piston.handler import BaseHandler
from piston.utils import rc, throttle, require_mime, validate
from piston.utils import Mimer, FormValidationError

from vumi.webapp.api.models import SentSMS, SentSMSBatch, ReceivedSMS, URLCallback
from vumi.webapp.api import forms
from vumi.webapp.api import signals
from vumi.webapp.api.utils import specify_fields

from alexandria.loader.base import YAMLLoader
from alexandria.dsl.utils import dump_menu

import pystache


class SendSMPPHandler(BaseHandler):
    allowed_methods = ('GET', 'POST',)
    exclude, fields = specify_fields(SentSMS,
        include=['transport_status_display'],
        exclude=['user','batch'])

    def _send_one(self, **kwargs):
        kwargs.update({
            'transport_name': 'smpp'
        })
        form = forms.SentSMSForm(kwargs)
        if not form.is_valid():
            raise FormValidationError(form)
        send_sms = form.save()
        logging.debug('Scheduling an SMPP to: %s' % kwargs['to_msisdn'])
        return send_sms

    @throttle(6000, 60) # allow for 100 a second
    def create(self, request):
        batch = SentSMSBatch.objects.create(title='', user=request.user)
        returnable = [self._send_one(
                                batch=batch.pk,
                                user=request.user.pk,
                                to_msisdn=msisdn,
                                from_msisdn=request.POST.get('from_msisdn'),
                                message=request.POST.get('message'))
                    for msisdn in request.POST.getlist('to_msisdn')]
        signals.sms_batch_scheduled.send(sender=SentSMSBatch, instance=batch,
                pk=batch.pk)
        return {"send_group":batch.pk,
                "group_list":batch.sentsms_set.all()}

