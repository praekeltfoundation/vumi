# -*- test-case-name: vumi.transports.api.tests.test_api -*-
import json

from twisted.python import log
from twisted.internet.defer import inlineCallbacks

from vumi.transports.httprpc import HttpRpcTransport


class HttpApiTransport(HttpRpcTransport):
    """
    Native HTTP API for getting messages into vumi.

    NOTE: This has no security. Put it behind a firewall or something.

    Configuration Values
    --------------------
    web_path : str
        The path relative to the host where this listens
    web_port : int
        The port this listens on
    transport_name : str
        The name this transport instance will use to create its queues
    reply_expected : boolean (default False)
        True if a reply message is expected.

    If reply_expected is True, the transport will wait for a reply message
    and will return the reply's content as the HTTP response body. If False,
    the message_id of the dispatched incoming message will be returned.
    """

    transport_type = 'http_api'

    def setup_transport(self):
        self.reply_expected = self.config.get('reply_expected', False)
        return super(HttpApiTransport, self).setup_transport()

    def handle_outbound_message(self, message):
        if self.reply_expected:
            return super(HttpApiTransport, self).handle_outbound_message(
                message)
        log.msg("HttpApiTransport dropping outbound message: %s" % (message))

    @inlineCallbacks
    def handle_raw_inbound_message(self, message_id, request):
        content = str(request.args.get('content', [None])[0])
        to_addr = str(request.args.get('to_addr', [None])[0])
        from_addr = str(request.args.get('from_addr', [None])[0])
        log.msg('HttpApiTransport sending from %s to %s message "%s"' % (
                    from_addr, to_addr, content))
        yield self.publish_message(
            message_id=message_id,
            content=content,
            to_addr=to_addr,
            from_addr=from_addr,
            provider='vumi',
            transport_type=self.transport_type,
        )
        if not self.reply_expected:
            yield self.finish_request(message_id,
                                      json.dumps({'message_id': message_id}))
