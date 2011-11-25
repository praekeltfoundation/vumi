# -*- test-case-name: vumi.transports.yo_ug_http.tests.test_yo_ug_http -*-
# -*- encoding: utf-8 -*-

from urllib import urlencode

from twisted.python import log
from twisted.internet.defer import inlineCallbacks
from twisted.internet.error import ConnectionRefusedError

from vumi.transports.base import Transport
from vumi.utils import http_request_full, normalize_msisdn
from twisted.internet import defer
defer.setDebugging(True)

class YoUgHttpTransport(Transport):
    
    def setup_transport(self):
        log.msg("Setup yo transport %s" % self.config)
        
    @inlineCallbacks
    def handle_outbound_message(self, message):
        log.msg("Outbound message to be processed %s" % repr(message))
        params = {
            'ybsacctno': self.config['ybsacctno'],
            'password': self.config['password'],
            'origin': message['from_addr'],
            'sms_content': message['content'],
            'destinations': message['to_addr'],
        }
        log.msg('Hitting %s with %s' % (self.config['url'], urlencode(params)))
        try:
            response = yield http_request_full(
                "%s?%s" % (self.config['url'], urlencode(params)), 
                "",
                {'User-Agent': ['Vumi Yo Transport'],
                 'Content-Type': ['application/json;charset=UTF-8'],},
                'GET')
        except ConnectionRefusedError:
            log.msg("Connection failed sending message: %s" % message)
            raise TemporaryFailure('connection refused')
        except Exception as ex:
            log.msg("Unexpected error %s" % repr(ex))
            
        if response.code != 200:
            log.msg("Error returned %s: %s" % (response.code, response.delivered_body))
            raise PermanentFailure('server error: HTTP %s: %s'
                                   % (response.code, response.delivered_body))
        
        log.msg("Sms received and accepted by Yo %s" % response.delivered_body)
        yield self.publish_ack(
            user_message_id=message['message_id'],
            sent_message_id="abc",
        )
    
    def stop_worker(self):
        log.msg("stop yo transport")