from twisted.internet.defer import inlineCallbacks, returnValue


class FakeXMLRPCService(object):

    def __init__(self, callback):
        self.callback = callback

    @inlineCallbacks
    def callRemote(self, *args, **kwargs):
        mocked_response = yield self.callback(*args, **kwargs)
        returnValue(mocked_response)
