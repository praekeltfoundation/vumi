from xmlrpclib import ServerProxy
from datetime import datetime, timedelta

OPERA_URL = 'https://dragon.sa.operatelecom.com:1089/Gateway'


class Opera(object):
    """Gateway for communicating with the Opera"""
    def __init__(self, service_id, password, channel, url=OPERA_URL,
                 verbose=False):
        self.proxy = ServerProxy(url, verbose=verbose)
        self.default_values = {
            'Service': service_id,
            'Password': password,
            'Channel': channel,
        }

    def send_sms(self, msisdns, smstexts, delivery=None, expiry=None, \
                        priority='standard', receipt='Y'):
        """Send a bulk of smses SMS"""

        struct = self.default_values.copy()
        delivery = delivery or datetime.utcnow()
        expiry = expiry or (delivery + timedelta(days=1))

        struct['Numbers'] = ','.join(map(str, msisdns))
        struct['SMSTexts'] = smstexts
        struct['Delivery'] = delivery
        struct['Expiry'] = expiry
        struct['Priority'] = priority
        struct['Receipt'] = receipt

        proxy_response = self.proxy.EAPIGateway.SendSMS(struct)
        return [{
            'msisdn': msisdn,
            'smstext': smstext,
            'delivery': struct['Delivery'],
            'expiry': struct['Expiry'],
            'priority': struct['Priority'],
            'receipt': struct['Receipt'],
            'identifier': proxy_response['Identifier'],
        } for (msisdn, smstext) in zip(msisdns, smstexts)]
