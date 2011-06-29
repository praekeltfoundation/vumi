from django import forms
from vumi.webapp.api.models import (SentSMS, URLCallback, ReceivedSMS,
                                    SMPPLink, SMPPResp, SentSMSBatch)


class SentSMSBatchForm(forms.ModelForm):
    class Meta:
        model = SentSMSBatch


class SentSMSForm(forms.ModelForm):
    class Meta:
        model = SentSMS


class SMPPLinkForm(forms.ModelForm):
    class Meta:
        model = SMPPLink


class SMPPRespForm(forms.ModelForm):
    class Meta:
        model = SMPPResp


class URLCallbackForm(forms.ModelForm):
    class Meta:
        model = URLCallback


class ReceivedSMSForm(forms.ModelForm):
    class Meta:
        model = ReceivedSMS


class SMSReceiptForm(forms.Form):
    user_id = forms.IntegerField(required=False)
    cliMsgId = forms.IntegerField()
    apiMsgId = forms.CharField(max_length=32)
    status = forms.IntegerField()
    timestamp = forms.IntegerField()
    to = forms.CharField(max_length=32)
    # would like to automatically validate from but the keyword is special
    # in python, leaving it 'required=False' for now
    _from = forms.CharField(max_length=32, required=False)
    charge = forms.FloatField()
