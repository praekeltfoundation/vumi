import base64
from django.test.client import Client
from vumi.webapp.api.models import SentSMS


class APIClient(Client):

    username = None
    password = None

    def request(self, **request):
        if ('HTTP_AUTHORIZATION' not in request) \
            and (self.username and self.password):
            b64 = base64.encodestring('%s:%s' % (
                self.username,
                self.password,
            )).strip()
            request.update({'HTTP_AUTHORIZATION': 'Basic %s' % b64})
        return super(APIClient, self).request(**request)

    def login(self, username, password):
        """Overridge the cookie based login of Client,
        we're using HTTP Basic Auth instead."""
        self.username = username
        self.password = password


def mock_sent_messages(user, count=1, to_msisdn='27123456789',
                       from_msisdn='27123456789', message='testing api',
                        **kwargs):
    return [SentSMS.objects.create(to_msisdn=to_msisdn,
                                   from_msisdn=from_msisdn,
                                   message=message,
                                   user=user,
                                   **kwargs) for i in range(0, count)]
