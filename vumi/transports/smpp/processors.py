from zope.interface import implements

from vumi.transports.smpp.helpers import (IDeliveryReportProcessor,
                                          IShortMessageProcessor)


class DeliveryReportProcessor(object):
    implements(IDeliveryReportProcessor)


class ShortMessageProcessor(object):
    implements(IShortMessageProcessor)
