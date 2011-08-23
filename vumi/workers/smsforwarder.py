from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.service import Worker
from vumi.message import Message
from vumi.webapp.api import utils


class SMSForwardWorker(Worker):

    @inlineCallbacks
    def startWorker(self):
        # create the publisher
        self.publisher = yield self.publish_to('sms.outbound.wasp.%s' %
                                                self.config['msisdn'])
        # when it's done, create the consumer and pass it the publisher
        self.consume("sms.inbound.wasp.%s" % self.config['msisdn'],
                        self.consume_message)

    def consume_message(self, message):
        dictionary = message.payload
        # specify a user per campaign
        user = User.objects.get(username=self.config['username'])
        profile = user.get_profile()
        urlcallback_set = profile.urlcallback_set.filter(name='sms_received')
        for urlcallback in urlcallback_set:
            try:
                params = [
                    ("callback_name", "sms_received"),
                    ("to_msisdn", str(dictionary.get('destination_addr'))),
                    ("from_msisdn", str(dictionary.get('source_addr'))),
                    ("message", str(dictionary.get('short_message')))
                ]
                url, resp = utils.callback(urlcallback.url, params)
                log.msg('RESP: %s' % repr(resp))
            except Exception, e:
                log.err(e)

        recipient = dictionary['sender']
        self.publisher.publish_message(Message(recipient=recipient,
                            message="Thanks, your SMS has been registered"))
