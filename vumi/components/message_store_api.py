# -*- test-case-name: vumi.components.tests.test_message_store_api -*-
from twisted.web import resource


class BatchSearchResource(resource.Resource):

    def __init__(self, message_store, batch_id):
        resource.Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id

    def render_POST(self, request):
        return self.batch_id

    def getChild(self, name, request):
        return self


class BatchResource(resource.Resource):

    def __init__(self, message_store, batch_id):
        resource.Resource.__init__(self)
        self.message_store = message_store
        self.batch_id = batch_id
        self.putChild('search', BatchSearchResource(message_store, batch_id))

    def render_GET(self, request):
        return self.batch_id

    def getChild(self, name, request):
        if not name:
            return self


class BatchIndexResource(resource.Resource):

    def __init__(self, message_store):
        resource.Resource.__init__(self)
        self.message_store = message_store

    def render_GET(self, request):
        return ''

    def getChild(self, batch_id, request):
        if batch_id:
            return BatchResource(self.message_store, batch_id)
        return self


class MessageStoreAPI(resource.Resource):

    def __init__(self, message_store):
        resource.Resource.__init__(self)
        self.putChild('batch', BatchIndexResource(message_store))
