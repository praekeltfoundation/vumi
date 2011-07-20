from twisted.internet import defer
from twisted.internet import reactor, protocol
from twisted.internet.defer import inlineCallbacks
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
from twisted.python import log

from vumi.utils import StringProducer
from vumi.workers.httprpc.transport import HttpRpcTransport
from vumi.webapp.api.utils import post_data_to_url

def httpRequest(url, data, headers={}, method='POST'):
    # Construct an Agent.
    agent = Agent(reactor)

    d = agent.request(method,
                      url,
                      Headers(headers),
                      StringProducer(data) if data else None)

    def handle_response(response):
        if response.code == 204:
            d = defer.succeed('')
        else:
            class SimpleReceiver(protocol.Protocol):
                def __init__(s, d):
                    s.buf = ''; s.d = d
                def dataReceived(s, data):
                    s.buf += data
                def connectionLost(s, reason):
                    # TODO: test if reason is twisted.web.client.ResponseDone, if not, do an errback
                    s.d.callback(s.buf)

            d = defer.Deferred()
            response.deliverBody(SimpleReceiver(d))
        return d

    d.addCallback(handle_response)
    return d


class IntegratTransport(HttpRpcTransport):
    
    @inlineCallbacks
    def finishRequest(self, uuid, message=''):
        log.msg('finishRequest for', uuid, "'%s'" % message)
        data = str(message).strip()
        log.msg("HttpRpcTransport.finishRequest with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.setHeader('Content-Type', 'text/xml; charset=utf-8')
            if data:
                request.setHeader('Content-Length', len(data))
                request.write(data)
            else:
                request.setResponseCode(204)
            request.finish()
            
            if data:
                yield httpRequest(
                    self.config['url'],data,
                    headers={'Content-Type': ['text/xml; charset:utf-8']}
                )
            
            del self.requests[uuid]

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")
    