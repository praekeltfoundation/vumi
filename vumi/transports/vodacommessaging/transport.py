import uuid

from twisted.internet.defer import inlineCallbacks
from twisted.python import log
from twisted.web import http
from twisted.web.server import NOT_DONE_YET
from vumi.message import Message
from vumi.transports.httprpc.transport import (
        HttpRpcTransport,
        HttpRpcHealthResource,
        HttpRpcResource)


class VodaMessHttpRpcResource(HttpRpcResource):

    def render_(self, request, http_action=None):
        log.msg("VodaMessHttpRpcResource HTTP Action: %s  args: %s" % (
            http_action, request.args))
        request.setHeader("content-type", "text/plain")
        uu = str(uuid.uuid4().get_hex())
        content = str(request.args.get('request', [None])[0])
        msisdn = str(request.args.get('msisdn', [None])[0])
        session = str(request.args.get('ussdSessionId', [None])[0])
        self.transport.publish_message(
                message_id=uu,
                content=content,
                to_addr=session,
                from_addr=msisdn,
                transport_name=self.transport.config.get('transport_name'),
                transport_type=self.transport.config.get('transport_type')
                )
        self.transport.requests[uu] = request
        return NOT_DONE_YET


class VodaMessHttpRpcTransport(HttpRpcTransport):

    @inlineCallbacks
    def setup_transport(self):
        self.uuid = uuid.uuid4()
        log.msg("Starting VodaMessHttpRpcTransport %s config: %s" % (self.uuid,
                                                             self.config))
        self.requests = {}

        # start receipt web resource
        self.receipt_resource = yield self.start_web_resources(
            [
                (VodaMessHttpRpcResource(self), self.config['web_path']),
                (HttpRpcHealthResource(self), 'health'),
            ],
            self.config['web_port'])

