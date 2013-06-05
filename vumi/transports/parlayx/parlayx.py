# -*- test-case-name: vumi.transports.parlayx.tests.test_parlayx -*-
from twisted.internet import defer

from vumi.transports.base import Transport
from vumi.config import ConfigText
from vumi import log


class ParlayXTransportConfig(Transport.CONFIG_CLASS):
    extra_config = ConfigText('Some Config var', default='foo')


class ParlayXTransport(Transport):

    CONFIG_CLASS = ParlayXTransportConfig
    transport_type = 'sms'

    def setup_transport(self):
        config = self.get_static_config()
        log.info('Transport starting with: %s' % (config.extra_config,))
        return defer.succeed(1)

    def teardown_transport(self):
        return defer.succeed(1)

    def handle_outbound_message(self, message):
        # message is a vumi.message.TransportUserMessage
        log.info('Sending %r' % (message.to_json(),))
        if message['content'] == 'fail!':
            return self.publish_nack(message['message_id'], 'failed')
        return self.publish_ack(message['message_id'], 'remote-message-id')
