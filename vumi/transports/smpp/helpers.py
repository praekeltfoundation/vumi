from zope.interface import Interface


class IDeliveryReportProcessor(Interface):

    def on_delivery_report(pdu):
        """Handle a delivery report PDU from the networks.

        This should always return a Deferred.
        All helpers should implement this even if it does nothing.
        """


class ISmProcessor(Interface):

    def on_deliver_sm(pdu):
        """Handle a short message PDU from the networks.

        This should always return a Deferred.
        All helpers should implement this even if it does nothing.
        """
