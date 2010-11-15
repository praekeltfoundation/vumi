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
    allowed_methods = ('POST',)
    
    @throttle(6000, 60) # allow for 100 a second
    @validate(forms.SMSReceiptForm)
    def create(self, request):
        logging.debug('Got notified of a delivered SMS to: %s' % request.POST['to'])
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
        except SentSMS.DoesNotExist, e:
            return rc.NOT_FOUND
    


class SendSMSHandler(BaseHandler):
    allowed_methods = ('GET', 'POST',)
    exclude, fields = specify_fields(SentSMS, 
        include=['transport_status_display'],
        exclude=['user'])
    
    def _send_one(self, **kwargs):
        kwargs.update({
            'transport_name': 'Clickatell'
        })
        form = forms.SentSMSForm(kwargs)
        if not form.is_valid():
            raise FormValidationError(form)
        send_sms = form.save()
        logging.debug('Scheduling an SMS to: %s' % kwargs['to_msisdn'])
        signals.sms_scheduled.send(sender=SentSMS, instance=send_sms,
                                    pk=send_sms.pk)
        return send_sms
    
    @throttle(6000, 60) # allow for 100 a second
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
    """
    FIXME: My eyes bleed
    """
    allowed_methods = ('POST',)
    exclude, fields = specify_fields(SentSMS, 
        include=['transport_status_display'],
        exclude=['user', re.compile(r'^_user_cache')])
    
    def _render_and_send_one(self, to_msisdn, from_msisdn, user_id, \
                                template, context):
        logging.debug('Scheduling an SMS to: %s' % to_msisdn)
        form = forms.SentSMSForm({
            'to_msisdn': to_msisdn,
            'from_msisdn': from_msisdn,
            'message': template.render(context=context),
            'user': user_id,
            'transport_name': 'Clickatell'
        })
        if not form.is_valid():
            raise FormValidationError(form)
        send_sms = form.save()
        signals.sms_scheduled.send(sender=SentSMS, instance=send_sms, 
                                    pk=send_sms.pk)
        return send_sms
    
    @throttle(6000, 60) # allow for 100 a second
    def create(self, request):
        template_string = request.POST.get('template')
        template = pystache.Template(template_string)
        msisdn_list = request.POST.getlist('to_msisdn')
        # not very happy with this template prefix filtering
        context_list = [(key.replace('template_',''), 
                            request.POST.getlist(key)) 
                                for key in request.POST
                                if key.startswith('template_')]
        # check if the nr of entries match
        if not all([len(value) == len(msisdn_list) 
                        for key, value in context_list]):
            response = rc.BAD_REQUEST
            response.content = "Number of to_msisdns and template variables" \
                                " do not match"
            return response
        responses = []
        for msisdn in msisdn_list:
            context = dict([(var_name, var_value_list.pop())
                                for var_name, var_value_list 
                                in context_list])
            send_sms = self._render_and_send_one(
                to_msisdn=msisdn, 
                from_msisdn=request.POST.get('from_msisdn'), 
                user_id=request.user.pk,
                template=template,
                context=context)
            responses.append(send_sms)
        return responses

class ReceiveSMSHandler(BaseHandler):
    allowed_methods = ('POST',)
    model = ReceivedSMS
    exclude = ('user',)
    
    @throttle(6000, 60) # allow for 100 a second
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
    


