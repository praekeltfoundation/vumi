from zope.interface import Interface


class IDeliveryReportProcessor(Interface):

    def on_delivery_report_pdu(pdu):
        """Handle a delivery report PDU from the networks.

        This should always return a Deferred.
        All helpers should implement this even if it does nothing.
        """

    def on_delivery_report_content(receipted_message_id, message_state):
        """Handle an unpacked delivery report from the networks.
        This can happen with certain SMSCs that don't set the necessary
        delivery report flags on a PDU. As a result we only detect the
        DR by matching a received SM against a predefined regex.
        """


class ISmProcessor(Interface):

    def on_short_message(pdu):
        """Handle a short message PDU from the networks.

        This should always return a Deferred.
        All helpers should implement this even if it does nothing.
        """
