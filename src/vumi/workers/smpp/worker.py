from twisted.python import log
from twisted.python.log import logging
from twisted.internet.defer import inlineCallbacks, returnValue

from vumi.service import Worker, Consumer, Publisher

class SMSKeywordConsumer(Consumer):
    exchange_name = "vumi"
    exchange_type = "direct"
    durable = True
    delivery_mode = 2
    queue_name = "sms.clickatell.keywords"
    
    def consume_json(self, dictionary):
        message = dictionary.get('short_message')
        head = message.split(' ')[0]
        try:
            user = models.User.objects.get(username=head)
        except:
            user= None
        if user:
            profile = user.get_profile()
            urlcallback_set = profile.urlcallback_set.filter(name='sms_received')
            for urlcallback in urlcallback_set:
                try:
                    url = urlcallback.url
                    log.msg('URL: %s' % urlcallback.url)
                    params = [
                            ("route", dictionary.get('destination_addr')),
                            ("msisdn", dictionary.get('source_addr')),
                            ("message", dictionary.get('short_message')),
                            ("json",
                                '{"route":"%s", "msisdn":"%s", "message":"%s"}' % (
                                dictionary.get('destination_addr'),
                                dictionary.get('source_addr'),
                                dictionary.get('short_message')))
                            ]
                    url, resp = utils.callback(url, params)
                    log.msg('RESP: %s' % resp)
                except Exception, e:
                    log.err(e)
        else:
            log.msg("Couldn't find user for message: %s" % message)
        log.msg("DELIVER SM %s" % (json.dumps(dictionary)))


class VodacomSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = "sms.278200702230015"
    
class MTNSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = 'sms.278326451590115'

class CellCSMSKeywordConsumer(SMSKeywordConsumer):
    routing_key = 'sms.278400317700115'

class SMSKeywordWorker(Worker):
    """
    A worker that fires off URLCallback's for incoming SMSs
    with keywords
    """
    
    @inlineCallbacks
    def startWorker(self):
        log.msg("Starting the SMSKeywordWorker, config: %s" % self.config)
        for consumer_class in [
            VodacomSMSKeywordConsumer, 
            MTNSMSKeywordConsumer, 
            CellCSMSKeywordConsumer]:
            yield self.start_consumer(consumer_class)
    
    def stopWorker(self):
        log.msg("Stopping the SMSKeywordWorker")
    