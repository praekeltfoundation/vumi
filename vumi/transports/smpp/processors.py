from zope.interface import implements

from vumi.transports.smpp.helpers import (IDeliveryReportProcessor,
                                          IShortMessageProcessor)


class DeliveryReportProcessor(object):
    implements(IDeliveryReportProcessor)

    def __init__(self, redis):
        self.redis = redis


class ShortMessageProcessor(object):
    implements(IShortMessageProcessor)

    def __init__(self, redis):
        self.redis = redis
