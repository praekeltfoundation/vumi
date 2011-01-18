import logging
from django.conf import settings
from celery.task import Task
from celery.task.http import HttpDispatchTask
from vumi.webapp.api.models import SentSMS, ReceivedSMS
from vumi.webapp.api.utils import callback

from clickatell.api import Clickatell
from clickatell.response import OKResponse, ERRResponse

# simple wrapper for Opera's XML-RPC service
from vumi.webapp.api.gateways.opera.backend import Opera
from vumi.webapp.api.gateways.e_scape.backend import E_Scape
from vumi.webapp.api.gateways.techsys.backend import Techsys

class SendSMSTask(Task):
    routing_key = 'vumi.webapp.sms.send'
    serializer = 'json'
    
    def send_sms_with_clickatell(self, send_sms):
        logger = self.get_logger(pk=send_sms.pk)
        clickatell = Clickatell(settings.CLICKATELL_USERNAME,
                                settings.CLICKATELL_PASSWORD, 
                                settings.CLICKATELL_API_ID,
                                sendmsg_defaults=settings.CLICKATELL_DEFAULTS['sendmsg'])
        [resp] = clickatell.sendmsg(recipients=[send_sms.to_msisdn],
                            sender=send_sms.from_msisdn,
                            text=send_sms.message,
                            climsgid=send_sms.pk)
        logger.debug("Clickatell delivery: %s" % resp)
        if isinstance(resp, OKResponse):
            send_sms.transport_msg_id = resp.value
            send_sms.save()
            return resp
        else:
            logger.debug("Retrying...")
            self.retry(args=[send_sms.pk], kwargs={})

    def send_sms_with_opera(self, send_sms):
        try:
            logger = self.get_logger(pk=send_sms.pk)
            opera = Opera(service_id=settings.OPERA_SERVICE_ID, 
                            password=settings.OPERA_PASSWORD,
                            channel=settings.OPERA_CHANNEL,
                            verbose=settings.DEBUG)
            [result] = opera.send_sms(
                msisdns=[send_sms.to_msisdn], 
                smstexts=[send_sms.message])
            send_sms.transport_msg_id = result['identifier']
            send_sms.save()
            return result
        except Exception,e:
            logger.debug('Retrying...')
            self.retry(args=[send_sms.pk], kwargs={})
    
    def send_sms_with_e_scape(self, send_sms):
        try:
            logger = self.get_logger(pk=send_sms.pk)
            e_scape = E_Scape(settings.E_SCAPE_API_ID)
            [result] = e_scape.send_sms(
                smsc = settings.E_SCAPE_SMSC,
                sender = send_sms.from_msisdn,
                recipients = [send_sms.to_msisdn],
                text = send_sms.message
            )
            send_sms.save()
            return result
        except Exception, e:
            logger.debug('Retrying...')
            self.retry(args=[send_sms.pk], kwargs={})
    
    def send_sms_with_techsys(self, send_sms):
        try:
            techsys = Techsys(settings.TECH_SYS_SMS_GATEWAY_URL,
                                settings.TECH_SYS_SMS_GATEWAY_BIND)
            result = techsys.send_sms(
                recipient=send_sms.to_msisdn,
                text=send_sms.message
            )
            send_sms.save()
            return result
        except Exception, e:
            logger.debug('Retrying')
            self.retry(args=[send_sms.pk], kwargs={})
    
    def run(self, pk):
        """
        FIXME:  preferably we'd have different queues for different transports.
                This isn't currently the case, if one transport is faster
                than the other this setup could slow down the fast one because 
                they're delayed by SMSs going to the slow transport.
        """
        send_sms = SentSMS.objects.get(pk=pk)
        dispatch = {
            'clickatell': self.send_sms_with_clickatell,
            'opera': self.send_sms_with_opera,
            'e-scape': self.send_sms_with_e_scape,
            'techsys': self.send_sms_with_techsys,
        }
        dispatcher = dispatch.get(send_sms.transport_name.lower())
        if dispatcher:
            return dispatcher(send_sms)
        else:
            logger = self.get_logger(pk=send_sms.pk)
            logger.error('No dispatchers available for transport %s' 
                            % send_sms.transport_name)

class ReceiveSMSTask(Task):
    routing_key = 'vumi.webapp.sms.receive'
    
    """FIXME: We can probably use the HttpDispatchTask instead of this"""
    def run(self, pk):
        received_sms = ReceivedSMS.objects.get(pk=pk)
        keys_and_values = received_sms.as_tuples()
        profile = received_sms.user.get_profile()
        urlcallback_set = profile.urlcallback_set.filter(name='sms_received')
        resp = [callback(urlcallback.url, keys_and_values)
                    for urlcallback in urlcallback_set]
        return resp
        


class DeliveryReportTask(Task):
    routing_key = 'vumi.webapp.sms.receipt'
    
    """FIXME: We can probably use the HttpDispatchTask instead of this"""
    def run(self, pk, receipt):
        sent_sms = SentSMS.objects.get(pk=pk)
        profile = sent_sms.user.get_profile()
        urlcallback_set = profile.urlcallback_set.filter(name='sms_receipt')
        return [callback(urlcallback.url, receipt.entries())
                    for urlcallback in urlcallback_set]
        

