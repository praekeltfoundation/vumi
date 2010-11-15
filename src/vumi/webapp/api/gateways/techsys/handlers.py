import re, yaml, logging
from datetime import datetime, timedelta

from piston.handler import BaseHandler
from piston.utils import rc, throttle, require_mime, validate
from piston.utils import Mimer, FormValidationError

from vumi.webapp.api.models import SentSMS, ReceivedSMS, URLCallback
from vumi.webapp.api import forms
from vumi.webapp.api import signals
from vumi.webapp.api.utils import specify_fields

from alexandria.loader.base import YAMLLoader
from alexandria.dsl.utils import dump_menu

import pystache

class SMSReceiptHandler(BaseHandler):
    # not implemented, gateway doesn't support it
    allowed_methods = ()


class SendSMSHandler(BaseHandler):
    allowed_methods = ('GET', 'POST',)
    exclude, fields = specify_fields(SentSMS, 
        include=['transport_status_display'],
        exclude=['user'])
    
    def _send_one(self, **kwargs):
        kwargs.update({
            'transport_name': 'Techsys'
        })
        form = forms.SentSMSForm(kwargs)
        if not form.is_valid():
            raise FormValidationError(form)
        send_sms = form.save()
        logging.debug('Scheduling an SMS to: %s' % kwargs['to_msisdn'])
        signals.sms_scheduled.send(sender=SentSMS, instance=send_sms,
                                    pk=send_sms.pk)
        return send_sms
    
    @throttle(60, 60) # allow for 1 a second
    def create(self, request):
        return [self._send_one(user=request.user.pk, 
                                to_msisdn=msisdn,
                                from_msisdn=request.POST.get('from_msisdn'),
                                message=request.POST.get('message'))
                    for msisdn in request.POST.getlist('to_msisdn')]
    
    @classmethod
    def transport_status_display(kls, instance):
        return instance.transport_status
    
    @throttle(60, 60)
    def read(self, request, sms_id=None):
        if sms_id:
            return self._read_one(request, sms_id)
        elif 'id' in request.GET:
            return self._read_filtered(request, request.GET.getlist('id'))
        else:
            since = request.GET.get('since', datetime.now() - timedelta(days=30))
            start = request.GET.get('start', 0)
            return self._read_from_point_in_time(request, start, since)
        
    def _read_one(self, request, sms_id):
        return request.user.sentsms_set.get(pk=sms_id)
    
    def _read_filtered(self, request, ids):
        return request.user.sentsms_set.filter(pk__in=map(int, ids))
    
    def _read_from_point_in_time(self, request, start, since):
        qs = request.user.sentsms_set.filter(updated_at__gte=since)
        return qs[start:start+100]
    

class SendTemplateSMSHandler(BaseHandler):
    # not implemented, gateway doesn't support it
    allowed_methods = ()

class ReceiveSMSHandler(BaseHandler):
    # not implemented, gateway doesn't support it
    allowed_methods = ()
