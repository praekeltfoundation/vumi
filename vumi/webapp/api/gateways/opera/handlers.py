import logging
from datetime import datetime

from django.http import HttpResponse
from django.utils import simplejson

from piston.handler import BaseHandler
from piston.utils import rc, throttle
from piston.utils import FormValidationError

from vumi.webapp.api.models import SentSMS, ReceivedSMS
from vumi.webapp.api import forms, signals
from vumi.webapp.api.utils import require_content_type

import iso8601

from vumi.transports.opera.utils import (parse_receipts_xml,
                                         parse_post_event_xml,
                                         OPERA_TIMESTAMP_FORMAT)


class SMSReceiptHandler(BaseHandler):
    allowed_methods = ('POST',)

    # TODO: Add Form validation for XML input
    @throttle(6000, 60)  # allow for 100 a second
    def create(self, request):
        receipts = parse_receipts_xml(request.raw_post_data)
        success, fail = [], []
        for receipt in receipts:
            try:
                # internally we store MSISDNs without a leading plus,
                # strip that from the msisdn
                sms = SentSMS.objects.get(
                    transport_name="Opera",
                    transport_msg_id=receipt.reference,
                    to_msisdn=receipt.msisdn.replace("+", ""))
                sms.transport_status = receipt.status
                sms.delivery_timestamp = datetime.strptime(receipt.timestamp,
                                                        OPERA_TIMESTAMP_FORMAT)
                sms.save()

                signals.sms_receipt.send(sender=SentSMS, instance=sms,
                                         pk=sms.pk, receipt=receipt._asdict())
                success.append(receipt)
            except SentSMS.DoesNotExist, error:
                logging.error(error)
                fail.append(receipt)

        return HttpResponse(simplejson.dumps({
            'success': map(lambda rcpt: rcpt._asdict(), success),
            'fail': map(lambda rcpt: rcpt._asdict(), fail)
        }), status=201, content_type='application/json; charset=utf-8')


class ReceiveSMSHandler(BaseHandler):
    allowed_methods = ('POST',)
    model = ReceivedSMS
    exclude = ('user',)

    @throttle(6000, 60)  # allow for 100 a second
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
