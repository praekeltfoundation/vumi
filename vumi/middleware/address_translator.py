from vumi.middleware import BaseMiddleware


class AddressTranslationMiddleware(BaseMiddleware):
    def setup_middleware(self):
        self.outbound_map = self.config.get('outbound_map')
        self.inbound_map = dict((v, k) for k, v in self.outbound_map.items())

    def handle_outbound(self, message, endpoint):
        fake_addr = message['to_addr']
        real_addr = self.outbound_map.get(fake_addr)
        if real_addr is not None:
            message['to_addr'] = real_addr
        return message

    def handle_inbound(self, message, endpoint):
        real_addr = message['from_addr']
        fake_addr = self.inbound_map.get(real_addr)
        if fake_addr is not None:
            message['from_addr'] = fake_addr
        return message
