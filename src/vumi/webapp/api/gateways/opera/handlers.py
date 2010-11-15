import re, yaml, logging
from datetime import datetime, timedelta

from django.http import HttpResponse
from django.utils import simplejson

from piston.handler import BaseHandler
from piston.utils import rc, throttle, require_mime, validate
from piston.utils import Mimer, FormValidationError

from vumi.webapp.api.models import SentSMS, ReceivedSMS, URLCallback
from vumi.webapp.api import forms, signals
from vumi.webapp.api.utils import require_content_type, specify_fields

import pystache
import iso8601

from utils import parse_receipts_xml, parse_post_event_xml, OPERA_TIMESTAMP_FORMAT

class SMSReceiptHandler(BaseHandler):
    allowed_methods = ('POST',)
    
    # TODO: Add Form validation for XML input
    @throttle(6000, 60) # allow for 100 a second
    @require_mime('xml')
    def create(self, request):
        receipts = parse_receipts_xml(request.raw_post_data)
        success, fail = [], []
        for receipt in receipts:
            try:
                # internally we store MSISDNs without a leading plus, strip that
                # from the msisdn
                sms = SentSMS.objects.get(
                                        transport_name = "Opera",
                                        transport_msg_id=receipt.reference, 
                                        to_msisdn=receipt.msisdn.replace("+",""))
                sms.transport_status = receipt.status
                sms.delivery_timestamp = datetime.strptime(receipt.timestamp, 
                                                        OPERA_TIMESTAMP_FORMAT)
                sms.save()
                
                signals.sms_receipt.send(sender=SentSMS, instance=sms, 
                                            pk=sms.pk, 
                                            receipt=receipt._asdict())
                success.append(receipt)
            except SentSMS.DoesNotExist, error:
                logging.error(error)
                fail.append(receipt)
                
        
        return HttpResponse(simplejson.dumps({
            'success': map(lambda rcpt: rcpt._asdict(), success),
            'fail': map(lambda rcpt: rcpt._asdict(), fail)
        }), status=201, content_type='application/json; charset=utf-8')
    


class SendSMSHandler(BaseHandler):
    allowed_methods = ('GET', 'POST',)
    exclude, fields = specify_fields(SentSMS, 
        include=['transport_status_display'],
        exclude=['user'])
    
    def _send_one(self, **kwargs):
        kwargs.update({
            'transport_name': 'Opera'
        })
        form = forms.SentSMSForm(kwargs)
        if not form.is_valid():
            raise FormValidationError(form)
        send_sms = form.save()
        logging.debug('Scheduling an SMS to: %s' % send_sms.to_msisdn)
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
            'transport_name': 'Opera'
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
    @require_content_type('text/plain')
    def create(self, request):
        sms = parse_post_event_xml(request.raw_post_data)
        # update the POST to have the `_from` key copied from `from`. 
        # The model has `_from` defined because `from` is a protected python
        # statement
        form = forms.ReceivedSMSForm({
            'user': request.user.pk,
            'to_msisdn': sms['Local'],
            'from_msisdn': sms['Remote'],
            'message': sms['Text'],
            'transport_name': 'Opera',
            'received_at': iso8601.parse_date(sms['ReceiveDate'])
        })
        if not form.is_valid():
            raise FormValidationError(form)
        
        receive_sms = form.save()
        logging.debug('Receiving an SMS from: %s' % receive_sms.from_msisdn)
        signals.sms_received.send(sender=ReceivedSMS, instance=receive_sms, 
                                    pk=receive_sms.pk)
        
        # return the response we got back to Opera, it could be re-routed
        # to other services in a callback chain.
        response = rc.ALL_OK
        response.content = request.raw_post_data
        response['Content-Type'] = 'text/xml; charset=utf8'
        return response
    


