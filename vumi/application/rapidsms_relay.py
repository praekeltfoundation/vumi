# -*- test-case-name: vumi.application.tests.test_rapidsms_relay -*-
from urllib2 import urlparse
from urllib import urlencode
import json

from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import LoopingCall

from vumi.application.base import ApplicationWorker
from vumi.utils import http_request_full
from vumi import log


class RapidSMSRelayApplication(ApplicationWorker):
    """Application that relays messages to RapidSMS.

    RapidSMS relay configuration options:

    :param str rapidsms_url:
        URL of the rapidsms instance. The receive API is expected to be
        at `<rapidsms_url>/router/receive`.
    """

   #("^router/can_send/(?P<message_id>\d+)/", can_send),

    def validate_config(self):
        self.rapidsms_url = self.config['rapidsms_url']
        self.rapidsms_password = self.config.get('rapidsms_password')
        self.rapidsms_backend = self.config.get('rapidsms_backend', 'vumi')
        self.delivered_on_ack = self.config.get('delivered_on_ack', False)
        self.outbox_poll_period = int(self.config.get('outbox_poll_period',
                                                      60))
        self._outbox_polling = LoopingCall(self._poll_outbox)

    def setup_application(self):
        self._outbox_polling.start(self.outbox_poll_period)

    def _poll_outbox(self):
        d = self._rapidsms_call("router/outbox")
        d.addCallback(lambda response: json.loads(response.delivered_body))
        d.addCallback(self._process_outbox)
        return d

    def _process_outbox(self, outbox):
        log.info("Polled outbox: %r" % (outbox.get('status', 'No status')))
        for message in outbox.get('outbox', []):
            pass
            # TODO: call .send_to()
            #return dict(
            #        id=self.pk,
            #        contact=self.connection.identity,
            #        backend=self.connection.backend.name,
            #        direction=self.direction,
            #        status=self.status,
            #        text=self.text,
            #        date=self.date.isoformat())

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
