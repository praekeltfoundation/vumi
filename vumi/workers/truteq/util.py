from twisted.internet import defer
from twisted.python import log
from twisted.python.log import logging
from ssmi import client


def ussd_code_to_routing_key(ussd_code):
    # convert *120*663*79# to s120s663s79h since
    # * and # are wildcards for AMQP based routing
    ussd_code = ussd_code.replace("*", "s")
    ussd_code = ussd_code.replace("#", "h")
    return ussd_code


class SessionType(object):
    """ussd_type's from SSMI documentation"""
    NEW = client.SSMI_USSD_TYPE_NEW
    EXISTING = client.SSMI_USSD_TYPE_EXISTING
    END = client.SSMI_USSD_TYPE_END
    TIMEOUT = client.SSMI_USSD_TYPE_TIMEOUT


class VumiSSMIProtocol(client.SSMIClient):
    """
    Subclassing the protocol to avoid me having to
    work with callbacks to do authorization
    """
    def __init__(self, username, password):
        self._username = username  # these could probably be a
        self._password = password  # callback instance var

    def set_handler(self, handler):
        # set the variables needed by SSMIClient so I don't have to
        # specify callbacks manually for each function
        self.handler = handler
        self._ussd_callback = self.handler.ussd_callback
        self._sms_callback = self.handler.sms_callback
        self._errback = self.handler.errback
        # ugh, can't do normal super() call because twisted's protocol.Factory
        # is an old style class that doesn't subclass object.
        client.SSMIClient.__init__(self)

    def connectionMade(self, *args, **kwargs):
        client.SSMIClient.connectionMade(self, *args, **kwargs)
        self.factory.onConnectionMade.callback(self)

    def connectionLost(self, *args, **kwargs):
        client.SSMIClient.connectionLost(self, *args, **kwargs)
        self.factory.onConnectionLost.callback(self)


class VumiSSMIFactory(client.SSMIFactory):
    """
    Subclassed the factory to allow me to work with my custom subclassed
    protocol
    """
    protocol = VumiSSMIProtocol

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        self.onConnectionMade = defer.Deferred()
        self.onConnectionLost = defer.Deferred()

    def buildProtocol(self, addr):
        prot = self.protocol(*self.args, **self.kwargs)
        prot.factory = self
        log.msg('SSMIFactory Connected.', logLevel=logging.DEBUG)
        log.msg('SSMIFactory Resetting reconnection delay',
                logLevel=logging.DEBUG)
        self.resetDelay()
        return prot
