from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from twisted.python import log
from vumi.transports.base import Transport
from vumi.utils import get_deploy_int
from uuid import uuid4
import gammu
import redis


class GSMTransport(Transport):
    """
    GSM Transport for use with Gammu_.
    If a `gammu` section is provided in the configuration file then that will
    be used, if not then gammu will read `~/.gammurc` as per the Gammu docs.

    Sample config file::

        transport_name: gsm_transport
        poll_interval: 120 # seconds, defaults to 60
        country_code: 27 # the international phone number country prefix
        phone_number: 27761234567 # the phone number of the SIM
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

    redis_outbound_queue = 'outbound_queue'

    def validate_config(self):
        """
        Make sure the config values are all present
        and are valid
        """
        self.redis_config = self.config('redis', {})
        self.gammu_config = self.config('gammu')
        self.poll_interval = int(self.config.get('poll_interval', 60))
        self.country_code = int(self.config.get('country_code'))
        self.phone_number = self.config.get('phone_number')
        self.phone = None

    def setup_transport(self):
        dbindex = get_deploy_int(self._amqp_client.vhost)
        redis_config = self.config.get('redis', {})
        self.r_server = yield redis.Redis(db=dbindex, **redis_config)
        self.r_prefix = "%(transport_name)s" % self.config

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
        self.phone = yield connect_phone()
        yield disconnect_phone(self.phone)
        self.phone = None

    def noop(self, *args, **kwargs):
        log.msg('Doing nothing with %s and %s' % (args, kwargs))

    def receive_messages(self, phone):
        message = phone.GetNextSMS(0, True)
        if not message:
            return

        dispatch_map = {
            'Deliver': self.receive_message,
            'Status_Report': self.receive_delivery_report,
        }

        handler = dispatch_map.get(message['Type'], self.noop)
        handler(message)

    def receive_message(self, message)
        self.publish_message(
            to_addr=normalize_msisdn(message['Number'],
                        country_code=self.country_code),
            from_addr=normalize_msisdn(self.phone_number,
                        country_code=self.country_code),
            content=message['Text'],
            transport_type='sms',
            message_id=uuid4(),
            transport_metadata={
                # when it was received on the modem
                'received_at': message['DateTime'],
                # when it was retrieved from the modem
                'read_at': message['SMSCDateTime'],
            })

    @inlineCallbacks
    def teardown_transport(self):
        self.poller.stop()
        if self.phone:

            def cb_modem_not_connected(failure):
                failure.trap(gammu.ERR_NOTCONNECTED)
                log.err()

            deferred = disconnect_phone(self.phone)
            deferred.addErrback(cb_modem_not_connected)
            yield deferred

    def handle_outbound_message(self, message):
        """
        Send an outbound message out as an SMS
        via the GSM Modem.

        Since we only poll the modem every `poll_interval` seconds
        there's the possibility of messages being lost when the transport
        restarts if messages have been ack'd off the queue but
        haven't been sent out through the phone yet.

        As a result we stash them in redis and whenever we have a connection
        to the phone we pull them out again and send them off.
        """
        self.r_server.rpush(self.redis_outbound_queue, message.to_json())