from zope.interface import Interface


class IDeliveryReportProcessor(Interface):

    def handle_delivery_report_pdu(pdu_data):
        """
        Handle a delivery report PDU from the networks.

        This should always return a Deferred that fires with a ``True`` if a
        delivery report was found and handled and ``False`` if that was not the
        case.

        All processors should implement this even if it does nothing.
        """

    def handle_delivery_report_content(pdu_data):
        """
        Handle an unpacked delivery report from the networks.

        This can happen with certain SMSCs that don't set the necessary
        delivery report flags on a PDU. As a result we only detect the DR by
        matching a received SM against a predefined regex.
        """


class IDeliverShortMessageProcessor(Interface):

    def handle_short_message_pdu(pdu):
        """
        Handle a short message PDU from the networks after it has been
        re-assembled and decoded.

        This should always return a Deferred that fires ``True`` or ``False``
        depending on whether the PDU was handled succesfully.

        All processors should implement this even if it does nothing.
        """

    def handle_multipart_pdu(pdu):
        """
        Handle a part of a multipart PDU.

        This should always return a Deferred that fires ``True`` or ``False``
        depending on whether the PDU was a multipart-part.

        All processors should implement this even if it does nothing.
        """

    def handle_ussd_pdu(pdu):
        """
        Handle a USSD pdu.

        This should always return a Deferred that fires ``True`` or ``False``
        depending on whether the PDU had a PDU payload.

        NOTE: It is likely that the USSD bits of this Interface will move to
              its own Interface implementation once work starts on an USSD over
              SMPP implementation.

        All processors should implement this even if it does nothing.
        """

    def decode_pdus(pdus):
        """
        Decode a list of PDUs and return the contents for each PDU's
        ``short_message`` field.
        """

    def dcs_decode(obj, data_coding):
        """
        Decode a byte string and return the unicode string for it according
        to the specified data coding.
        """


class ISubmitShortMessageProcessor(Interface):

    def handle_raw_outbound_message(vumi_message, esme_protocol):
        """
        Handle an outbound message from Vumi by calling the appropriate
        methods on the protocol with the appropriate parameters.

        These parameters and values can differ per MNO.

        Should return a Deferred that fires with a the list of sequence_numbers
        returning from the submit_sm calls.
        """
