import gammu
from vumi.transports.gsm import GSMTransport
from vumi.tests.utils import FakeRedis
from twisted.internet.task import LoopingCall
from twisted.python import log


class FakeGammuPhone(object):

    def __init__(self, messages=[]):
        self.messages = messages
        self.outbox = []

    def Init(self):
        log.msg('Called Init')

    def SetConfig(self, index, config):
        log.msg('Called SetConfig with %s and %s' % (index, config))
        self.config_index = index
        self.config = config

    def Terminate(self):
        log.msg('Terminate called')

    def GetNextSMS(self, folder, start, location=None):
        if not self.messages:
            log.msg('Messages buffer is empty, returning empty list')
            return []

        if start:
            return [self.messages[0]]
        else:
            for message in self.messages:
                if message['Location'] > location:
                    return [message]

    def DeleteSMS(self, folder, location):
        log.msg('DeleteSMS called with %s, %s' % (folder, location))
        for index, message in enumerate(self.messages):
            if message['Location'] == location:
                return self.messages.pop(index)
        raise gammu.GSMError, 'message not found'

    def SendSMS(self, message):
        log.msg('Sending SMS %s' % (message,))
        self.outbox.append(message)

class FailingFakeGammuPhone(FakeGammuPhone):
    def SendSMS(self, message):
        raise gammu.GSMError, 'Fail!'

class FakeGSMTransport(GSMTransport):

    def setup_transport(self):
        log.msg('Setting up the fake transport with FakeRedis')
        self.r_server = FakeRedis()
        self.r_prefix = "%(transport_name)s" % self.config
