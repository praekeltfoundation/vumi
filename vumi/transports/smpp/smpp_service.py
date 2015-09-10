from twisted.internet.defer import Deferred, succeed

from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.smpp.protocol import EsmeProtocol, EsmeProtocolFactory
from vumi.transports.smpp.sequence import RedisSequence


class SmppService(ReconnectingClientService):

    sequence_class = RedisSequence

    def __init__(self, endpoint, bind_type, transport):
        self.transport = transport
        self.transport_name = transport.transport_name
        self.message_stash = self.transport.message_stash
        self.deliver_sm_processor = self.transport.deliver_sm_processor
        self.dr_processor = self.transport.dr_processor
        self.sequence_generator = self.sequence_class(transport.redis)

        self.wait_on_protocol_deferreds = []
        factory = EsmeProtocolFactory(self, bind_type)
        ReconnectingClientService.__init__(self, endpoint, factory)

    def clientConnected(self, protocol):
        ReconnectingClientService.clientConnected(self, protocol)
        while self.wait_on_protocol_deferreds:
            deferred = self.wait_on_protocol_deferreds.pop()
            deferred.callback(protocol)

    def get_protocol(self):
        if self._protocol is not None:
            return succeed(self._protocol)
        else:
            d = Deferred()
            self.wait_on_protocol_deferreds.append(d)
            return d

    def get_bind_state(self):
        if self._protocol is None:
            return EsmeProtocol.CLOSED_STATE
        return self._protocol.state

    def is_bound(self):
        if self._protocol is not None:
            return self._protocol.is_bound()
        return False

    def stopService(self):
        if self._protocol is not None:
            d = self._protocol.disconnect()
            d.addCallback(
                lambda _: ReconnectingClientService.stopService(self))
            return d
        return ReconnectingClientService.stopService(self)

    def get_config(self):
        return self.transport.get_static_config()

    def on_smpp_bind(self):
        return self.transport.unpause_connectors()

    def on_connection_lost(self):
        return self.transport.pause_connectors()

    def get_submit_sm_callback(self, pdu_status):
        return {
            'ESME_ROK': self.transport.handle_submit_sm_success,
            'ESME_RTHROTTLED': self.transport.handle_submit_sm_throttled,
            'ESME_RMSGQFUL': self.transport.handle_submit_sm_throttled,
        }.get(pdu_status, self.transport.handle_submit_sm_failure)
