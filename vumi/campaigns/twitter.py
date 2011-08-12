from twisted.internet.defer import inlineCallbacks
from twisted.python import log

from twittytwister import twitter

from vumi.workers.integrat.worker import IntegratWorker
from vumi.utils import safe_routing_key


class TwitterUSSDTransport(IntegratWorker):

    @inlineCallbacks
    def startWorker(self):
        """docstring for startWorker"""
        self.publisher = yield self.publish_to(
            'ussd.outbound.%(transport_name)s' % self.config)
        self.twitter = twitter.Twitter(self.config['username'],
                                       self.config['password'])
        self.consumer = yield self.consume('ussd.inbound.%s.%s' % (
            self.config['transport_name'],
            safe_routing_key(self.config['ussd_code'])
        ), self.consume_message)

    def new_session(self, data):
        session_id = data['transport_session_id']
        self.reply(session_id,
                   'Whose latest twitter mention do you want to see?')

    @inlineCallbacks
    def resume_session(self, data):
        session_id = data['transport_session_id']
        search_term = data['message'].replace('@', '')

        def got_entry(msg):
            log.msg(msg)
            self.end(session_id, msg.title)

        yield self.twitter.search('@%s' % search_term, got_entry, {
            'rpp': '1'
        })
