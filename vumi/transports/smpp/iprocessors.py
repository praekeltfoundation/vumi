from zope.interface import Interface


class IDeliveryReportProcessor(Interface):

    def handle_delivery_report_pdu(pdu_data):
        """Handle a delivery report PDU from the networks.

        This should always return a Deferred that fires with a ``True``
        if a delivery report was found and handled and ``False`` if that
        was not the case.

        All helpers should implement this even if it does nothing.
        """

    def inspect_delivery_report_content(content):
        """Inspect content received in a short message and return an
        object that can be passed through to
        ``handle_delivery_report_content`` if a delivery report was
        found

        Returns ``None`` if no delivery report was found.
        """

    def handle_delivery_report_content(pdu_data):
        """Handle an unpacked delivery report from the networks.
        This can happen with certain SMSCs that don't set the necessary
        delivery report flags on a PDU. As a result we only detect the
        DR by matching a received SM against a predefined regex.
        """


class IDeliverShortMessageProcessor(Interface):

    def handle_short_message_pdu(pdu):
        """Handle a short message PDU from the networks after it has been
        re-assembled and decoded.

        Certain SMSCs submit delivery reports like normal SMSs, if
        that's the case then this ``IDeliverShortMessageProcessor``
        would need to call
        ``IDeliveryReportProcessor.inspect_delivery_report_content` to
        see if that is the case and handle appropriately if that is the
        case.

        This should always return a Deferred.
        All helpers should implement this even if it does nothing.
        """
