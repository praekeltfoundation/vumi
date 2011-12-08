from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from vumi.transports.base import Transport
import gammu


class GSMTransport(Transport):
    """
    GSM Transport for use with Gammu_.
    If a `gammu` section is provided in the configuration file then that will
    be used, if not then gammu will read `~/.gammurc` as per the Gammu docs.

    Sample config file::

        transport_name: gsm_transport
        poll_interval: 120 # seconds, defaults to 60
        gammu:
            UseGlobalDebugFile: 0,
            DebugFile: ''
            SyncTime: 0
            Connection: 'at'
            LockDevice: 0
            DebugLevel: ''
            Device: '/dev/cu.HUAWEIMobile-Modem'
            StartInfo': 0,
            Model: ''

    .. _Gammu: http://www.wammu.eu

    """

    def validate_config(self):
        """
        Make sure the config values are all present
        and are valid
        """
        self.gammu_config = self.config('gammu')
        self.poll_interval = int(self.config.get('poll_interval', 60))

    def setup_transport(self):
        self.poller = LoopingCall(self.check_for_new_messages)
        self.poller.start(self.receive_and_send_messages, True)

    @inlineCallbacks
    def connnect_phone(self):
        phone = gammu.StateMachine()
        if self.gammu_config:
            phone.SetConfig(self.gammu_config)
        else:
            phone.ReadConfig()
        yield deferToThread(phone.Init)
        returnValue(phone)

    @inlineCallbacks
    def disconnect_phone(self, phone):
        yield deferToThread(phone.Terminate)

    @inlineCallbacks
    def receive_and_send_messages(self):
        phone = yield connect_phone()
        yield disconnect_phone(phone)


    def teardown_transport(self):
        pass

    def handle_outbound_message(self, message):
        """
        Send an outbound message out as an SMS
        via the GSM Modem
        """
        pass