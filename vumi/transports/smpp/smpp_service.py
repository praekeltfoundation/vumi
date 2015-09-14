from functools import wraps

from twisted.internet.defer import succeed

from vumi.reconnecting_client import ReconnectingClientService
from vumi.transports.smpp.protocol import (
    EsmeProtocol, EsmeProtocolFactory, EsmeProtocolError)
from vumi.transports.smpp.sequence import RedisSequence


def proxy_protocol(func):
    @wraps(func)
    def wrapper(self, *args, **kw):
        protocol = self.get_protocol()
        if protocol is None:
            raise EsmeProtocolError('%s called while not connected.' % (func,))
        return func(self, protocol, *args, **kw)
    return wrapper


class SmppService(ReconnectingClientService):

    throttle_statuses = ('ESME_RTHROTTLED', 'ESME_RMSGQFUL')

    def __init__(self, endpoint, bind_type, transport):
        self.transport = transport
        self.transport_name = transport.transport_name
        self.message_stash = self.transport.message_stash
        self.deliver_sm_processor = self.transport.deliver_sm_processor
        self.dr_processor = self.transport.dr_processor
        self.sequence_generator = RedisSequence(transport.redis)

        factory = EsmeProtocolFactory(self, bind_type)
        ReconnectingClientService.__init__(self, endpoint, factory)

    def get_protocol(self):
        return self._protocol

    def get_bind_state(self):
        if self._protocol is None:
            return EsmeProtocol.CLOSED_STATE
        return self._protocol.state

    def is_bound(self):
        if self._protocol is not None:
            return self._protocol.is_bound()
        return False

    def stopService(self):
        d = succeed(None)
        if self._protocol is not None:
            d.addCallback(lambda _: self._protocol.disconnect())
        d.addCallback(lambda _: ReconnectingClientService.stopService(self))
        return d

    def get_config(self):
        return self.transport.get_static_config()

    def on_smpp_bind(self):
        return self.transport.unpause_connectors()

    def on_connection_lost(self):
        return self.transport.pause_connectors()

    def handle_submit_sm_resp(self, message_id, smpp_message_id, pdu_status):
        if pdu_status in self.throttle_statuses:
            return self.handle_submit_sm_throttled(message_id)
        func = self.transport.handle_submit_sm_failure
        if pdu_status == 'ESME_ROK':
            func = self.transport.handle_submit_sm_success
        return func(message_id, smpp_message_id, pdu_status)

    def handle_submit_sm_throttled(self, message_id):
        return self.transport.handle_submit_sm_throttled(message_id)

    @proxy_protocol
    def submit_sm(self, protocol, *args, **kw):
        """
        See :meth:`EsmeProtocol.submit_sm`.
        """
        return protocol.submit_sm(*args, **kw)

    @proxy_protocol
    def submit_sm_long(self, protocol, *args, **kw):
        """
        See :meth:`EsmeProtocol.submit_sm_long`.
        """
        return protocol.submit_sm_long(*args, **kw)

    @proxy_protocol
    def submit_csm_sar(self, protocol, *args, **kw):
        """
        See :meth:`EsmeProtocol.submit_csm_sar`.
        """
        return protocol.submit_csm_sar(*args, **kw)

    @proxy_protocol
    def submit_csm_udh(self, protocol, *args, **kw):
        """
        See :meth:`EsmeProtocol.submit_csm_udh`.
        """
        return protocol.submit_csm_udh(*args, **kw)
