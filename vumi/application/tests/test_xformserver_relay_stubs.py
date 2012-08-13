from twisted.python import log
from twisted.web.resource import Resource
from twisted.web import http


class TestXformServer(Resource):
    isLeaf = True

    def __init__(self, code=http.OK, content='', headers={}, callback=None):
        self.code = code
        self.content = content
        self.headers = headers
        self.callback = callback

    def render_POST(self, request):
        content = request.content.read()
        #print content
        if self.callback:
            return self.callback(request)

        request.setResponseCode(self.code)
        for key, value in self.headers.items():
            request.setHeader(key, value)
        return self.content
