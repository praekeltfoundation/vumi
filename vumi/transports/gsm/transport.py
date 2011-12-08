from vumi.transports.base import Transport


class GSMTransport(Transport):

    def validate_config(self):
        """
        Make sure the config values are all present
        and are valid
        """
        pass

    def setup_transport(self):
        pass

    def teardown_transport(self):
        pass

    def handle_outbound_message(self, message):
        """
        Send an outbound message out as an SMS
        via the GSM Modem
        """
        pass