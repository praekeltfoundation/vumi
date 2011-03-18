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

class SendSMSHandler(BaseHandler):
    allowed_methods = ('GET', 'POST',)
    exclude, fields = specify_fields(SentSMS, 
        include=['transport_status_display'],
        exclude=['user'])
    
    def _send_one(self, **kwargs):
        
        # get the user's profile
        user = kwargs.get('user')
        profile = user.get_profile()
        transport = profile.transport_set.latest()
        
        kwargs.update({
            'transport_name': transport.name
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
        return [self._send_one(user=request.user, 
                                to_msisdn=msisdn,
                                from_msisdn=request.POST.get('from_msisdn'),
                                message=request.POST.get('message'),
                                )
                    for msisdn in request.POST.getlist('to_msisdn')]
    
    @classmethod
    def transport_status_display(kls, instance):
        """
        helper method to for human readable transport status display.
        """
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
        """Return a single SMS"""
        return request.user.sentsms_set.get(pk=sms_id)

    def _read_filtered(self, request, ids):
        """Return a list of SMSs with the given ids, maximum of 100 at a time"""
        return request.user.sentsms_set.filter(pk__in=map(int, ids[:100]))

    def _read_from_point_in_time(self, request, start, since):
        """Return 100 records starting at point `start` since timestamp `since`"""
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
    
    def _render_and_send_one(self, to_msisdn, from_msisdn, user, \
                                template, context):
        logging.debug('Scheduling an SMS to: %s' % to_msisdn)
        profile = uset.get_profile()
        transport = profile.transport_set.latest()
        form = forms.SentSMSForm({
            'to_msisdn': to_msisdn,
            'from_msisdn': from_msisdn,
            'message': template.render(context=context),
            'user': user_id,
            'transport_name': transport.name
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
                user=request.user,
                template=template,
                context=context)
            responses.append(send_sms)
        return responses

