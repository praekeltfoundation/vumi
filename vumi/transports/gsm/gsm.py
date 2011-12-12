# -*- test-case-name: vumi.transports.gsm.tests.test_gsm -*-
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.threads import deferToThread
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from twisted.python import log
from vumi.transports.base import Transport
from vumi.utils import get_deploy_int, normalize_msisdn
from vumi.message import TransportUserMessage
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
            UseGlobalDebugFile: 0
            DebugFile: ''
            SyncTime: 0
            Connection: 'at'
            LockDevice: 0
            DebugLevel: ''
            Device: '/dev/cu.HUAWEIMobile-Modem'
            StartInfo: 0
            Model: ''

    .. _Gammu: http://www.wammu.eu

    """

    redis_outbound_queue = 'outbound_queue'

    def validate_config(self):
        """
        Make sure the config values are all present
        and are valid
        """
        log.msg('Validating config')
        # Map message types to callbacks that handle that message type
        self.dispatch_map = {
            'Deliver': self.receive_message,
            'Status_Report': self.receive_delivery_report,
        }

        self.redis_config = self.config.get('redis', {})
        self.gammu_config = self.config.get('gammu')
        self.poll_interval = int(self.config.get('poll_interval', 60))
        self.country_code = str(self.config.get('country_code'))
        self.phone_number = str(self.config.get('phone_number'))
        self.phone = None

    @inlineCallbacks
    def setup_transport(self):
        log.msg('Setting up transport')
        dbindex = get_deploy_int(self._amqp_client.vhost)
        redis_config = self.config.get('redis', {})
        self.r_server = yield redis.Redis(db=dbindex, **redis_config)
        self.r_prefix = "%(transport_name)s" % self.config
        self.start_polling()

    def start_polling(self):
        phone = gammu.StateMachine()
        self.poller = LoopingCall(self.receive_and_send_messages, phone)
        self.poller.start(self.poll_interval, True)

    def noop(self, message):
        log.msg('Doing nothing with %s' % (message,))

    @inlineCallbacks
    def connect_phone(self, phone):
        log.msg('Connecting the phone')
        if self.gammu_config:
            phone.SetConfig(0, self.gammu_config)
        else:
            phone.ReadConfig()
        yield deferToThread(phone.Init)
        returnValue(phone)

    @inlineCallbacks
    def disconnect_phone(self, phone):
        log.msg('Disconnecting the phone')
        yield deferToThread(phone.Terminate)
        returnValue(phone)

    @inlineCallbacks
    def receive_and_send_messages(self, phone):
        log.msg('Receiving and sending messages')
        self.phone = yield self.connect_phone(phone)
        yield self.read_until_empty(self.phone)
        yield self.send_outbound(self.phone)
        yield self.disconnect_phone(self.phone)
        self.phone = None

    @inlineCallbacks
    def read_until_empty(self, phone, history=[]):
        if history:
            last_message = history[-1]
            list_with_message = yield self.get_next_sms(phone, False,
                                        last_message['Location'])
        else:
            list_with_message = yield self.get_next_sms(phone, True)

        if list_with_message:
            message = list_with_message[0]
            handler = self.dispatch_map.get(message['Type'], self.noop)
            yield handler(message)
            history.append(message)
            yield self.read_until_empty(phone, history)
        returnValue(history)

    @inlineCallbacks
    def get_next_sms(self, phone, start, location=None):
        # We use the flattened pseudo folder which means that all contents
        # of all folders are flattened into one 'fake' folder. That also
        # means that the messages we encounter aren't necessarily messages
        # we want to work with. Messages we do not know what to do
        # with we want to keep in the folder untouched. To do so we need to
        # increment the index we're reading from so as to work with the next
        # message in line.
        message = yield deferToThread(phone.GetNextSMS, 0, start, location)
        returnValue(message)

    @inlineCallbacks
    def receive_message(self, message):
        self.publish_message(
            to_addr=normalize_msisdn(str(message['Number']),
                        country_code=self.country_code),
            from_addr=normalize_msisdn(self.phone_number,
                        country_code=self.country_code),
            content=message['Text'],
            transport_type='sms',
            message_id=uuid4().get_hex(),
            transport_metadata={
                # when it was received on the modem
                'received_at': message['DateTime'],
                # when it was retrieved from the modem
                'read_at': message['SMSCDateTime'],
            })
        yield self.delete_message(self.phone, message)

    @inlineCallbacks
    def send_outbound(self, phone):
        while self.r_server.llen(self.redis_outbound_queue):
            json_data = self.r_server.lpop(self.redis_outbound_queue)
            message = TransportUserMessage.from_json(json_data)

            def _send_failure(f):
                self.send_failure(message, f.value, f.getTraceback())
                if self.SUPPRESS_FAILURE_EXCEPTIONS:
                    return None
                return f

            deferred = deferToThread(phone.SendSMS, {
                'Number': message['to_addr'],
                'Text': message['content'],
                'MessageReference': message['message_id'],
                # Send using the Phone's known SMSC
                'SMSC': {
                    'Location': 1
                },
                # this will create submit message with request for delivery report
                'Type': 'Status_Report',
            })
            deferred.addErrback(_send_failure)
            yield deferred

    @inlineCallbacks
    def receive_delivery_report(self, delivery_report):
        log.err('Receiving delivery report %s' % (delivery_report,))

        status_map = {
            'D': 'delivered',
            'd': 'delivered',
            'R': 'delivered',
            'Q': 'pending',
            'P': 'pending',
            'B': 'pending',
            'a': 'pending',
            'u': 'pending'
        }

        phone_status = delivery_report['DeliveryStatus']
        vumi_status = status_map.get(phone_status, 'failed')
        self.publish_delivery_report(delivery_report['MessageReference'],
            vumi_status)
        yield self.delete_message(self.phone, delivery_report)

    @inlineCallbacks
    def delete_message(self, phone, message):
        log.msg('Deleting %s from %s' % (message['Location'], phone))
        yield deferToThread(phone.DeleteSMS, 0, message['Location'])

    @inlineCallbacks
    def teardown_transport(self):
        # depending on when the shutdown was issued
        # these might not've been created yet.
        poller = getattr(self, 'poller', None)
        phone = getattr(self, 'phone', None)

        if poller and poller.running:
            poller.stop()

        if phone:
            def cb_modem_not_connected(failure):
                failure.trap(gammu.ERR_NOTCONNECTED)
                log.err()

            deferred = self.disconnect_phone(phone)
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