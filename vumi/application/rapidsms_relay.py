# -*- test-case-name: vumi.application.tests.test_rapidsms_relay -*-
from twisted.internet.defer import inlineCallbacks
from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi import log
from urllib2 import urlparse
from urllib import urlencode


class RapidSMSRelayApplication(ApplicationWorker):
    """Application that relays messages to RapidSMS.

    RapidSMS relay configuration options:

    :param str rapidsms_url:
        URL of the rapidsms instance. The receive API is expected to be
        at `<rapidsms_url>/router/receive`.
    """

   #("^router/outbox", outbox),
   #("^router/can_send/(?P<message_id>\d+)/", can_send),

    def validate_config(self):
        self.rapidsms_url = self.config['rapidsms_url']
        self.rapidsms_password = self.config.get('rapidsms_password')
        self.rapidsms_backend = self.config.get('rapidsms_backend', 'vumi')
        self.delivered_on_ack = self.config.get('delivered_on_ack', False)

    def _rapidsms_call(self, route, **kw):
        if self.rapidsms_password is not None:
            kw['password'] = self.rapidsms_password
        url = urlparse.urljoin(self.rapidsms_url, route)
        url = "%s?%s" % (url, urlencode(kw))
        return http_request_full(url)

    def consume_user_message(self, message):
        # rapidsms limits the sender to 20 characters
        sender = message['from_addr'][:20]
        content = message['content'] or ''
        return self._rapidsms_call("router/receive",
                                   backend=self.rapidsms_backend,
                                   sender=sender,
                                   message=content,
                                   echo=False)

    def _delivered(self, event):
        msg_id = event['user_message_id']
        return self._rapidsms_call("router/delivered",
                                   message_id=msg_id)

    def consume_ack(self, event):
        if self.delivered_on_ack:
            return self._delivered(event)

    def consume_delivery_report(self, event):
        if not self.delivered_on_ack:
            if event['delivery_status'] == 'delivered':
                return self._delivered(event)
