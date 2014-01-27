
from twisted.python import log
from twisted.internet import defer

from vumi.worker import BaseWorker
from vumi.transports.smpp.deprecated.clientserver.server import (
    SmscServerFactory)
from vumi.transports.smpp.deprecated.transport import SmppTransportConfig
from vumi.config import ConfigServerEndpoint


class SmppServiceConfig(SmppTransportConfig):
    twisted_endpoint = ConfigServerEndpoint(
        'Server endpoint description', required=True, static=True)


class SmppService(BaseWorker):
    """
    The SmppService
    """
    CONFIG_CLASS = SmppServiceConfig

    def setup_connectors(self):
        pass

    @defer.inlineCallbacks
    def setup_worker(self):
        log.msg("Starting the SmppService")
        config = self.get_static_config()

        delivery_report_string = self.config.get('smsc_delivery_report_string')
        self.factory = SmscServerFactory(
            delivery_report_string=delivery_report_string)
        self.listening = yield config.twisted_endpoint.listen(self.factory)
