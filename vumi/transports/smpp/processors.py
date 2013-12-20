from zope.interface import implements

from vumi.transports.smpp.helpers import (IDeliveryReportProcessor,
                                          IDeliverShortMessageProcessor)


class DeliveryReportProcessor(object):
    implements(IDeliveryReportProcessor)

    def __init__(self, redis):
        self.redis = redis


class DeliverShortMessageProcessor(object):
    implements(IDeliverShortMessageProcessor)

    def __init__(self, redis):
        self.redis = redis
