from zope.interface import Interface


class IDeliveryReportProcessor(Interface):

    def inspect_delivery_report_pdu(pdu):
        """Inspect a PDU and return an object that can be passed straight
        through to ``handle_delivery_report_pdu`` if a delivery report
        was found.

        Returns ``None`` if no delivery report was found.
        """

    def handle_delivery_report_pdu(pdu_data):
        """Handle a delivery report PDU from the networks.

        This should always return a Deferred.
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
        """Handle a short message PDU from the networks.

        This should always return a Deferred.
        All helpers should implement this even if it does nothing.
        """
