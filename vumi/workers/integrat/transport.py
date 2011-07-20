from twisted.python import log
from vumi.workers.httprpc.transport import HttpRpcTransport

class IntegratTransport(HttpRpcTransport):
    
    def finishRequest(self, uuid, message=''):
        data = str(message)
        log.msg("HttpRpcTransport.finishRequest with data:", repr(data))
        log.msg(repr(self.requests))
        request = self.requests.get(uuid)
        if request:
            request.setHeader('Content-Type', 'text/xml; charset=utf-8')
            request.write(data)
            request.finish()
            del self.requests[uuid]

    def stopWorker(self):
        log.msg("Stopping the HttpRpcTransport")
    